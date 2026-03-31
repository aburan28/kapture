// Package hub implements the hub controller and gRPC server that provides
// global orchestration and visibility across agent clusters.
package hub

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

const (
	// DefaultHeartbeatInterval is sent to agents on registration.
	DefaultHeartbeatInterval = 30
	// AgentTimeout is how long after the last heartbeat an agent is considered disconnected.
	AgentTimeout = 90 * time.Second
)

// agentEntry tracks a connected agent in the in-memory registry.
type agentEntry struct {
	info          *hubv1.AgentInfo
	lastHeartbeat time.Time
	captures      map[string]*hubv1.CaptureStatusSummary // key: "namespace/name"
}

// Server implements hubv1.HubServiceServer and manages agent registration,
// heartbeats, and aggregated capture status.
type Server struct {
	hubv1.UnimplementedHubServiceServer

	mu     sync.RWMutex
	agents map[string]*agentEntry // key: agent_id

	grpcServer       *grpc.Server
	dashboardServer  *http.Server
	grpcAddress      string
	dashboardAddress string
}

// NewServer creates a new hub gRPC server and optional dashboard server.
func NewServer(grpcAddress, dashboardAddress string, opts ...grpc.ServerOption) *Server {
	s := &Server{
		agents:           make(map[string]*agentEntry),
		grpcAddress:      grpcAddress,
		dashboardAddress: dashboardAddress,
	}
	s.grpcServer = grpc.NewServer(opts...)
	hubv1.RegisterHubServiceServer(s.grpcServer, s)
	return s
}

// NewServerWithTLS creates a new hub gRPC server with TLS credentials.
func NewServerWithTLS(grpcAddress, dashboardAddress string, creds credentials.TransportCredentials) *Server {
	return NewServer(grpcAddress, dashboardAddress, grpc.Creds(creds))
}

// Start begins serving gRPC requests and the optional dashboard. It blocks until
// the gRPC server is stopped.
func (s *Server) Start(ctx context.Context) error {
	grpcListener, err := net.Listen("tcp", s.grpcAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.grpcAddress, err)
	}

	if s.dashboardAddress != "" {
		dashboardListener, err := net.Listen("tcp", s.dashboardAddress)
		if err != nil {
			return fmt.Errorf("failed to listen on %s: %w", s.dashboardAddress, err)
		}
		s.dashboardServer = newDashboardServer(s)
		go func() {
			if err := s.dashboardServer.Serve(dashboardListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				_ = dashboardListener.Close()
			}
		}()
	}

	go func() {
		<-ctx.Done()
		s.Stop()
	}()

	return s.grpcServer.Serve(grpcListener)
}

// Stop gracefully stops the gRPC server and dashboard server.
func (s *Server) Stop() {
	if s.dashboardServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.dashboardServer.Shutdown(ctx)
		cancel()
	}
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

// GRPCServer returns the underlying grpc.Server for testing or additional configuration.
func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

// DashboardAddress returns the bound dashboard address configuration.
func (s *Server) DashboardAddress() string {
	return s.dashboardAddress
}

// RegisterAgent registers an agent cluster with the hub.
func (s *Server) RegisterAgent(_ context.Context, req *hubv1.RegisterAgentRequest) (*hubv1.RegisterAgentResponse, error) {
	if req.GetAgentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "agent_id is required")
	}

	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	s.agents[req.AgentId] = &agentEntry{
		info: &hubv1.AgentInfo{
			AgentId:        req.AgentId,
			ClusterName:    req.ClusterName,
			ActiveCaptures: 0,
			LastHeartbeat:  timestamppb.New(now),
			Capabilities:   req.Capabilities,
			State:          hubv1.AgentState_AGENT_STATE_CONNECTED,
		},
		lastHeartbeat: now,
		captures:      make(map[string]*hubv1.CaptureStatusSummary),
	}

	return &hubv1.RegisterAgentResponse{
		Accepted:                 true,
		Message:                  "agent registered",
		HeartbeatIntervalSeconds: DefaultHeartbeatInterval,
	}, nil
}

// Heartbeat updates the last-seen timestamp for an agent.
func (s *Server) Heartbeat(_ context.Context, req *hubv1.HeartbeatRequest) (*hubv1.HeartbeatResponse, error) {
	if req.GetAgentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "agent_id is required")
	}

	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.agents[req.AgentId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "agent %q not registered", req.AgentId)
	}

	entry.lastHeartbeat = now
	entry.info.LastHeartbeat = timestamppb.New(now)
	entry.info.ActiveCaptures = req.ActiveCaptures
	entry.info.State = hubv1.AgentState_AGENT_STATE_CONNECTED

	for _, cs := range req.CaptureSummaries {
		key := captureKey(cs.CaptureNamespace, cs.CaptureName)
		entry.captures[key] = cs
	}

	return &hubv1.HeartbeatResponse{Acknowledged: true}, nil
}

// DeregisterAgent removes an agent from the registry.
func (s *Server) DeregisterAgent(_ context.Context, req *hubv1.DeregisterAgentRequest) (*hubv1.DeregisterAgentResponse, error) {
	if req.GetAgentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "agent_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.agents[req.AgentId]; !ok {
		return nil, status.Errorf(codes.NotFound, "agent %q not registered", req.AgentId)
	}

	delete(s.agents, req.AgentId)
	return &hubv1.DeregisterAgentResponse{Acknowledged: true}, nil
}

// WatchDirectives is a server-streaming RPC for hub-initiated capture commands.
// Currently a no-op that keeps the stream open until the client disconnects.
func (s *Server) WatchDirectives(req *hubv1.WatchDirectivesRequest, stream grpc.ServerStreamingServer[hubv1.WatchDirectivesResponse]) error {
	if req.GetAgentId() == "" {
		return status.Error(codes.InvalidArgument, "agent_id is required")
	}

	s.mu.RLock()
	_, ok := s.agents[req.AgentId]
	s.mu.RUnlock()
	if !ok {
		return status.Errorf(codes.NotFound, "agent %q not registered", req.AgentId)
	}

	<-stream.Context().Done()
	return stream.Context().Err()
}

// ReportCaptureStatus receives capture status updates from an agent.
func (s *Server) ReportCaptureStatus(_ context.Context, req *hubv1.ReportCaptureStatusRequest) (*hubv1.ReportCaptureStatusResponse, error) {
	if req.GetAgentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "agent_id is required")
	}
	if len(req.GetStatuses()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one status is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.agents[req.AgentId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "agent %q not registered", req.AgentId)
	}

	for _, cs := range req.Statuses {
		key := captureKey(cs.CaptureNamespace, cs.CaptureName)
		entry.captures[key] = cs
	}
	entry.info.ActiveCaptures = countInProgressCaptures(entry.captures)

	return &hubv1.ReportCaptureStatusResponse{Acknowledged: true}, nil
}

// ListCaptures returns all captures across agents, optionally filtered by agent_id.
func (s *Server) ListCaptures(_ context.Context, req *hubv1.ListCapturesRequest) (*hubv1.ListCapturesResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var captures []*hubv1.CaptureInfo
	for agentID, entry := range s.agents {
		if req.GetAgentId() != "" && agentID != req.AgentId {
			continue
		}
		for _, cs := range entry.captures {
			if req.GetNamespace() != "" && cs.CaptureNamespace != req.Namespace {
				continue
			}
			captures = append(captures, captureInfoFromSummary(cs, agentID))
		}
	}

	return &hubv1.ListCapturesResponse{Captures: captures}, nil
}

// GetCaptureStatus returns the status of a specific capture.
func (s *Server) GetCaptureStatus(_ context.Context, req *hubv1.GetCaptureStatusRequest) (*hubv1.GetCaptureStatusResponse, error) {
	if req.GetCaptureName() == "" || req.GetCaptureNamespace() == "" {
		return nil, status.Error(codes.InvalidArgument, "capture_name and capture_namespace are required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	key := captureKey(req.CaptureNamespace, req.CaptureName)

	if req.GetAgentId() != "" {
		entry, ok := s.agents[req.AgentId]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "agent %q not registered", req.AgentId)
		}
		cs, ok := entry.captures[key]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "capture %q not found on agent %q", key, req.AgentId)
		}
		return &hubv1.GetCaptureStatusResponse{
			Capture: captureInfoFromSummary(cs, req.AgentId),
		}, nil
	}

	for agentID, entry := range s.agents {
		if cs, ok := entry.captures[key]; ok {
			return &hubv1.GetCaptureStatusResponse{
				Capture: captureInfoFromSummary(cs, agentID),
			}, nil
		}
	}

	return nil, status.Errorf(codes.NotFound, "capture %q not found", key)
}

// ListAgents returns information about all registered agents.
func (s *Server) ListAgents(_ context.Context, _ *hubv1.ListAgentsRequest) (*hubv1.ListAgentsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	agents := make([]*hubv1.AgentInfo, 0, len(s.agents))
	now := time.Now()
	for _, entry := range s.agents {
		info := cloneAgentInfo(entry.info)
		if now.Sub(entry.lastHeartbeat) > AgentTimeout {
			info.State = hubv1.AgentState_AGENT_STATE_DISCONNECTED
		}
		agents = append(agents, info)
	}

	return &hubv1.ListAgentsResponse{Agents: agents}, nil
}

// ConnectedAgentCount returns the number of connected agents.
func (s *Server) ConnectedAgentCount() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.connectedAgentCountAt(time.Now())
}

// ActiveCaptureCount returns the total number of in-progress captures across all agents.
func (s *Server) ActiveCaptureCount() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var count int32
	for _, entry := range s.agents {
		count += entryActiveCaptures(entry)
	}
	return count
}

// AgentStatuses returns a snapshot of agent statuses for the CaptureHub CR status.
func (s *Server) AgentStatuses() []AgentStatusSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	result := make([]AgentStatusSnapshot, 0, len(s.agents))
	for _, entry := range s.agents {
		state := entry.info.State
		if now.Sub(entry.lastHeartbeat) > AgentTimeout {
			state = hubv1.AgentState_AGENT_STATE_DISCONNECTED
		}
		activeCount := entryActiveCaptures(entry)
		result = append(result, AgentStatusSnapshot{
			ID:             entry.info.AgentId,
			Name:           entry.info.ClusterName,
			LastHeartbeat:  entry.lastHeartbeat,
			ActiveCaptures: activeCount,
			State:          state,
			Capabilities:   append([]string(nil), entry.info.Capabilities...),
		})
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Name == result[j].Name {
			return result[i].ID < result[j].ID
		}
		return result[i].Name < result[j].Name
	})
	return result
}

// DashboardSnapshot returns the aggregated hub state used by the dashboard.
func (s *Server) DashboardSnapshot() DashboardSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	snapshot := DashboardSnapshot{
		GeneratedAt:        now,
		ConnectedAgents:    s.connectedAgentCountAt(now),
		ActiveCaptures:     0,
		InProgressCount:    0,
		Agents:             make([]AgentStatusSnapshot, 0, len(s.agents)),
		InProgressCaptures: []CaptureSnapshot{},
	}

	for _, entry := range s.agents {
		state := entry.info.State
		if now.Sub(entry.lastHeartbeat) > AgentTimeout {
			state = hubv1.AgentState_AGENT_STATE_DISCONNECTED
		}

		activeCount := entryActiveCaptures(entry)
		snapshot.Agents = append(snapshot.Agents, AgentStatusSnapshot{
			ID:             entry.info.AgentId,
			Name:           entry.info.ClusterName,
			LastHeartbeat:  entry.lastHeartbeat,
			ActiveCaptures: activeCount,
			State:          state,
			Capabilities:   append([]string(nil), entry.info.Capabilities...),
		})

		for _, cs := range entry.captures {
			if !isCaptureInProgress(cs.Phase) {
				continue
			}
			snapshot.InProgressCaptures = append(snapshot.InProgressCaptures, CaptureSnapshot{
				Name:             cs.CaptureName,
				Namespace:        cs.CaptureNamespace,
				AgentID:          entry.info.AgentId,
				ClusterName:      entry.info.ClusterName,
				Phase:            cs.Phase,
				CapturedRequests: cs.CapturedRequests,
				BytesWritten:     cs.BytesWritten,
				ReadyReplicas:    cs.ReadyReplicas,
				DesiredReplicas:  cs.DesiredReplicas,
			})
		}
	}

	snapshot.ActiveCaptures = int32(len(snapshot.InProgressCaptures))
	snapshot.InProgressCount = len(snapshot.InProgressCaptures)

	sort.Slice(snapshot.Agents, func(i, j int) bool {
		if snapshot.Agents[i].Name == snapshot.Agents[j].Name {
			return snapshot.Agents[i].ID < snapshot.Agents[j].ID
		}
		return snapshot.Agents[i].Name < snapshot.Agents[j].Name
	})
	sort.Slice(snapshot.InProgressCaptures, func(i, j int) bool {
		left := snapshot.InProgressCaptures[i]
		right := snapshot.InProgressCaptures[j]
		if left.ClusterName == right.ClusterName {
			if left.Namespace == right.Namespace {
				return left.Name < right.Name
			}
			return left.Namespace < right.Namespace
		}
		return left.ClusterName < right.ClusterName
	})

	return snapshot
}

// AgentStatusSnapshot is a plain Go struct used by the controller and dashboard.
type AgentStatusSnapshot struct {
	ID             string
	Name           string
	LastHeartbeat  time.Time
	ActiveCaptures int32
	State          hubv1.AgentState
	Capabilities   []string
}

// CaptureSnapshot is the dashboard-ready capture view model.
type CaptureSnapshot struct {
	Name             string
	Namespace        string
	AgentID          string
	ClusterName      string
	Phase            hubv1.CapturePhase
	CapturedRequests int64
	BytesWritten     int64
	ReadyReplicas    int32
	DesiredReplicas  int32
}

// DashboardSnapshot is the aggregated hub snapshot rendered by the dashboard.
type DashboardSnapshot struct {
	GeneratedAt        time.Time
	ConnectedAgents    int32
	ActiveCaptures     int32
	InProgressCount    int
	Agents             []AgentStatusSnapshot
	InProgressCaptures []CaptureSnapshot
}

func captureKey(namespace, name string) string {
	return namespace + "/" + name
}

func captureInfoFromSummary(cs *hubv1.CaptureStatusSummary, agentID string) *hubv1.CaptureInfo {
	return &hubv1.CaptureInfo{
		CaptureName:      cs.CaptureName,
		CaptureNamespace: cs.CaptureNamespace,
		AgentId:          agentID,
		Phase:            cs.Phase,
		CapturedRequests: cs.CapturedRequests,
		BytesWritten:     cs.BytesWritten,
		ReadyReplicas:    cs.ReadyReplicas,
		DesiredReplicas:  cs.DesiredReplicas,
	}
}

func cloneAgentInfo(info *hubv1.AgentInfo) *hubv1.AgentInfo {
	return &hubv1.AgentInfo{
		AgentId:        info.AgentId,
		ClusterName:    info.ClusterName,
		ActiveCaptures: info.ActiveCaptures,
		LastHeartbeat:  info.LastHeartbeat,
		Capabilities:   append([]string(nil), info.Capabilities...),
		State:          info.State,
	}
}

func (s *Server) connectedAgentCountAt(now time.Time) int32 {
	var count int32
	for _, entry := range s.agents {
		if now.Sub(entry.lastHeartbeat) <= AgentTimeout {
			count++
		}
	}
	return count
}

func countInProgressCaptures(captures map[string]*hubv1.CaptureStatusSummary) int32 {
	var count int32
	for _, capture := range captures {
		if isCaptureInProgress(capture.GetPhase()) {
			count++
		}
	}
	return count
}

func entryActiveCaptures(entry *agentEntry) int32 {
	if counted := countInProgressCaptures(entry.captures); counted > entry.info.ActiveCaptures {
		return counted
	}
	return entry.info.ActiveCaptures
}

func isCaptureInProgress(phase hubv1.CapturePhase) bool {
	return phase == hubv1.CapturePhase_CAPTURE_PHASE_PENDING || phase == hubv1.CapturePhase_CAPTURE_PHASE_ACTIVE
}
