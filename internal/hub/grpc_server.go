// Package hub implements the hub controller and gRPC server that provides
// global orchestration and visibility across spoke clusters.
package hub

import (
	"context"
	"fmt"
	"net"
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
	// DefaultHeartbeatInterval is sent to spokes on registration.
	DefaultHeartbeatInterval = 30
	// SpokeTimeout is how long after the last heartbeat a spoke is considered disconnected.
	SpokeTimeout = 90 * time.Second
	// DirectiveBufferSize is how many directives can be queued per spoke
	// before SendDirective starts rejecting with ResourceExhausted.
	DirectiveBufferSize = 32
	// StopGracePeriod bounds how long Stop waits for in-flight RPCs before
	// force-closing connections.
	StopGracePeriod = 5 * time.Second
)

// spokeEntry tracks a connected spoke in the in-memory registry.
type spokeEntry struct {
	info          *hubv1.SpokeInfo
	lastHeartbeat time.Time
	captures      map[string]*hubv1.CaptureStatusSummary // key: "namespace/name"
	replays       map[string]*hubv1.ReplayStatusSummary  // key: "namespace/name/replayName"

	// directives buffers hub-initiated directives until the spoke's
	// WatchDirectives stream picks them up. The channel is never closed;
	// done signals watchers that the entry has been retired.
	directives chan *hubv1.WatchDirectivesResponse
	done       chan struct{}
}

// Server implements hubv1.HubServiceServer and manages spoke registration,
// heartbeats, and aggregated capture status.
type Server struct {
	hubv1.UnimplementedHubServiceServer

	mu     sync.RWMutex
	spokes map[string]*spokeEntry // key: spoke_id

	// activeLoadTests is the authoritative CaptureLoadTest list pushed by
	// the CaptureHub reconciler from the CR store. It rides on heartbeat
	// responses so spokes can garbage-collect orphaned replay shards.
	// activeLoadTestsComplete is false until the reconciler has managed a
	// successful CR list, so a cold cache never looks like "no load tests".
	activeLoadTests         []*hubv1.LoadTestKey
	activeLoadTestsComplete bool

	grpcServer *grpc.Server
	address    string

	// shutdown is closed when the server stops so long-lived streaming
	// handlers (WatchDirectives) return instead of blocking GracefulStop.
	shutdown     chan struct{}
	shutdownOnce sync.Once
}

// NewServer creates a new hub gRPC server.
func NewServer(address string, opts ...grpc.ServerOption) *Server {
	s := &Server{
		spokes:   make(map[string]*spokeEntry),
		address:  address,
		shutdown: make(chan struct{}),
	}
	s.grpcServer = grpc.NewServer(opts...)
	hubv1.RegisterHubServiceServer(s.grpcServer, s)
	return s
}

// NewServerWithTLS creates a new hub gRPC server with TLS credentials.
func NewServerWithTLS(address string, creds credentials.TransportCredentials) *Server {
	return NewServer(address, grpc.Creds(creds))
}

// Start begins serving gRPC requests. It blocks until the server is stopped.
func (s *Server) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.address, err)
	}

	// Shut down when context is cancelled.
	go func() {
		<-ctx.Done()
		s.Stop()
	}()

	return s.grpcServer.Serve(lis)
}

// Stop stops the gRPC server. It first signals streaming handlers to
// return — GracefulStop alone would block forever on the long-lived
// WatchDirectives streams held by connected spokes — then falls back to a
// hard stop if graceful shutdown does not finish within StopGracePeriod.
func (s *Server) Stop() {
	if s.grpcServer == nil {
		return
	}
	s.shutdownOnce.Do(func() { close(s.shutdown) })

	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(StopGracePeriod):
		s.grpcServer.Stop()
		<-done
	}
}

// GRPCServer returns the underlying grpc.Server for testing or additional configuration.
func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

// --- Spoke lifecycle RPCs ---

// RegisterSpoke registers a spoke cluster with the hub.
func (s *Server) RegisterSpoke(ctx context.Context, req *hubv1.RegisterSpokeRequest) (*hubv1.RegisterSpokeResponse, error) {
	if req.GetSpokeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "spoke_id is required")
	}
	if err := verifySpokeIdentity(ctx, req.SpokeId); err != nil {
		return nil, err
	}

	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	// Retire any previous registration so stale directive watchers terminate.
	if existing, ok := s.spokes[req.SpokeId]; ok {
		close(existing.done)
	}

	s.spokes[req.SpokeId] = &spokeEntry{
		info: &hubv1.SpokeInfo{
			SpokeId:        req.SpokeId,
			ClusterName:    req.ClusterName,
			ActiveCaptures: 0,
			LastHeartbeat:  timestamppb.New(now),
			Capabilities:   req.Capabilities,
			State:          hubv1.SpokeState_SPOKE_STATE_CONNECTED,
			Cell:           req.Cell,
		},
		lastHeartbeat: now,
		captures:      make(map[string]*hubv1.CaptureStatusSummary),
		replays:       make(map[string]*hubv1.ReplayStatusSummary),
		directives:    make(chan *hubv1.WatchDirectivesResponse, DirectiveBufferSize),
		done:          make(chan struct{}),
	}

	return &hubv1.RegisterSpokeResponse{
		Accepted:                 true,
		Message:                  "spoke registered",
		HeartbeatIntervalSeconds: DefaultHeartbeatInterval,
	}, nil
}

// Heartbeat updates the last-seen timestamp for a spoke.
func (s *Server) Heartbeat(ctx context.Context, req *hubv1.HeartbeatRequest) (*hubv1.HeartbeatResponse, error) {
	if req.GetSpokeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "spoke_id is required")
	}
	if err := verifySpokeIdentity(ctx, req.SpokeId); err != nil {
		return nil, err
	}

	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.spokes[req.SpokeId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "spoke %q not registered", req.SpokeId)
	}

	entry.lastHeartbeat = now
	entry.info.LastHeartbeat = timestamppb.New(now)
	entry.info.ActiveCaptures = req.ActiveCaptures
	entry.info.State = hubv1.SpokeState_SPOKE_STATE_CONNECTED

	// Update capture summaries from heartbeat.
	for _, cs := range req.CaptureSummaries {
		key := captureKey(cs.CaptureNamespace, cs.CaptureName)
		entry.captures[key] = cs
	}

	// Update replay summaries from heartbeat.
	for _, rs := range req.ReplaySummaries {
		entry.replays[replayKey(rs)] = rs
	}
	entry.info.ActiveReplays = activeReplayCount(entry.replays)

	return &hubv1.HeartbeatResponse{
		Acknowledged:            true,
		ActiveLoadTests:         s.activeLoadTests,
		ActiveLoadTestsComplete: s.activeLoadTestsComplete,
	}, nil
}

// SetActiveLoadTests publishes the authoritative CaptureLoadTest list for
// heartbeat responses. complete must only be true when the list came from
// a successful CR store read.
func (s *Server) SetActiveLoadTests(keys []*hubv1.LoadTestKey, complete bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeLoadTests = keys
	s.activeLoadTestsComplete = complete
}

// DeregisterSpoke removes a spoke from the registry.
func (s *Server) DeregisterSpoke(ctx context.Context, req *hubv1.DeregisterSpokeRequest) (*hubv1.DeregisterSpokeResponse, error) {
	if req.GetSpokeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "spoke_id is required")
	}
	if err := verifySpokeIdentity(ctx, req.SpokeId); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.spokes[req.SpokeId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "spoke %q not registered", req.SpokeId)
	}

	close(entry.done)
	delete(s.spokes, req.SpokeId)
	return &hubv1.DeregisterSpokeResponse{Acknowledged: true}, nil
}

// WatchDirectives is a server-streaming RPC for hub-initiated capture commands.
// Directives queued via SendDirective/BroadcastDirective are streamed to the
// spoke until the client disconnects or the spoke is deregistered.
func (s *Server) WatchDirectives(req *hubv1.WatchDirectivesRequest, stream grpc.ServerStreamingServer[hubv1.WatchDirectivesResponse]) error {
	if req.GetSpokeId() == "" {
		return status.Error(codes.InvalidArgument, "spoke_id is required")
	}
	if err := verifySpokeIdentity(stream.Context(), req.SpokeId); err != nil {
		return err
	}

	s.mu.RLock()
	entry, ok := s.spokes[req.SpokeId]
	s.mu.RUnlock()
	if !ok {
		return status.Errorf(codes.NotFound, "spoke %q not registered", req.SpokeId)
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-s.shutdown:
			return status.Error(codes.Unavailable, "hub shutting down")
		case <-entry.done:
			return status.Errorf(codes.Aborted, "spoke %q deregistered", req.SpokeId)
		case resp := <-entry.directives:
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

// SendDirective queues a capture directive for delivery to a single spoke.
// The directive is buffered until the spoke's WatchDirectives stream
// consumes it.
func (s *Server) SendDirective(spokeID string, directive *hubv1.CaptureDirective) error {
	if directive == nil {
		return status.Error(codes.InvalidArgument, "directive is required")
	}
	return s.queueDirective(spokeID, &hubv1.WatchDirectivesResponse{Directive: directive})
}

// SendReplayDirective queues a replay directive for delivery to a single
// spoke. Used by the load test coordinator to start and stop replay shards.
func (s *Server) SendReplayDirective(spokeID string, directive *hubv1.ReplayDirective) error {
	if directive == nil {
		return status.Error(codes.InvalidArgument, "directive is required")
	}
	return s.queueDirective(spokeID, &hubv1.WatchDirectivesResponse{ReplayDirective: directive})
}

func (s *Server) queueDirective(spokeID string, resp *hubv1.WatchDirectivesResponse) error {
	if spokeID == "" {
		return status.Error(codes.InvalidArgument, "spoke_id is required")
	}

	s.mu.RLock()
	entry, ok := s.spokes[spokeID]
	s.mu.RUnlock()
	if !ok {
		return status.Errorf(codes.NotFound, "spoke %q not registered", spokeID)
	}

	select {
	case entry.directives <- resp:
		return nil
	default:
		return status.Errorf(codes.ResourceExhausted, "directive buffer full for spoke %q", spokeID)
	}
}

// BroadcastDirective queues a directive for every registered spoke and
// returns the number of spokes it was queued to. Spokes with a full
// directive buffer are skipped.
func (s *Server) BroadcastDirective(directive *hubv1.CaptureDirective) int {
	if directive == nil {
		return 0
	}

	s.mu.RLock()
	entries := make([]*spokeEntry, 0, len(s.spokes))
	for _, entry := range s.spokes {
		entries = append(entries, entry)
	}
	s.mu.RUnlock()

	queued := 0
	for _, entry := range entries {
		select {
		case entry.directives <- &hubv1.WatchDirectivesResponse{Directive: directive}:
			queued++
		default:
		}
	}
	return queued
}

// --- Status reporting ---

// ReportCaptureStatus receives capture status updates from a spoke.
func (s *Server) ReportCaptureStatus(ctx context.Context, req *hubv1.ReportCaptureStatusRequest) (*hubv1.ReportCaptureStatusResponse, error) {
	if req.GetSpokeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "spoke_id is required")
	}
	if err := verifySpokeIdentity(ctx, req.SpokeId); err != nil {
		return nil, err
	}
	if len(req.GetStatuses()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one status is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.spokes[req.SpokeId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "spoke %q not registered", req.SpokeId)
	}

	for _, cs := range req.Statuses {
		key := captureKey(cs.CaptureNamespace, cs.CaptureName)
		entry.captures[key] = cs
	}
	entry.info.ActiveCaptures = int32(len(entry.captures))

	return &hubv1.ReportCaptureStatusResponse{Acknowledged: true}, nil
}

// ReportReplayStatus receives replay shard status updates from a spoke.
func (s *Server) ReportReplayStatus(ctx context.Context, req *hubv1.ReportReplayStatusRequest) (*hubv1.ReportReplayStatusResponse, error) {
	if req.GetSpokeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "spoke_id is required")
	}
	if err := verifySpokeIdentity(ctx, req.SpokeId); err != nil {
		return nil, err
	}
	if len(req.GetStatuses()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one status is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.spokes[req.SpokeId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "spoke %q not registered", req.SpokeId)
	}

	for _, rs := range req.Statuses {
		entry.replays[replayKey(rs)] = rs
	}
	entry.info.ActiveReplays = activeReplayCount(entry.replays)

	return &hubv1.ReportReplayStatusResponse{Acknowledged: true}, nil
}

// --- Query RPCs ---

// ListCaptures returns all captures across spokes, optionally filtered by spoke_id.
func (s *Server) ListCaptures(_ context.Context, req *hubv1.ListCapturesRequest) (*hubv1.ListCapturesResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var captures []*hubv1.CaptureInfo
	for spokeID, entry := range s.spokes {
		if req.GetSpokeId() != "" && spokeID != req.SpokeId {
			continue
		}
		if req.GetCell() != "" && entry.info.Cell != req.Cell {
			continue
		}
		for _, cs := range entry.captures {
			if req.GetNamespace() != "" && cs.CaptureNamespace != req.Namespace {
				continue
			}
			captures = append(captures, captureInfoFromSummary(cs, spokeID))
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

	// If spoke_id is given, look up directly.
	if req.GetSpokeId() != "" {
		entry, ok := s.spokes[req.SpokeId]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "spoke %q not registered", req.SpokeId)
		}
		cs, ok := entry.captures[key]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "capture %q not found on spoke %q", key, req.SpokeId)
		}
		return &hubv1.GetCaptureStatusResponse{
			Capture: captureInfoFromSummary(cs, req.SpokeId),
		}, nil
	}

	// Search all spokes for the capture.
	for spokeID, entry := range s.spokes {
		if cs, ok := entry.captures[key]; ok {
			return &hubv1.GetCaptureStatusResponse{
				Capture: captureInfoFromSummary(cs, spokeID),
			}, nil
		}
	}

	return nil, status.Errorf(codes.NotFound, "capture %q not found", key)
}

// ListSpokes returns information about all registered spokes, optionally
// filtered by cell.
func (s *Server) ListSpokes(_ context.Context, req *hubv1.ListSpokesRequest) (*hubv1.ListSpokesResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	spokes := make([]*hubv1.SpokeInfo, 0, len(s.spokes))
	now := time.Now()
	for _, entry := range s.spokes {
		if req.GetCell() != "" && entry.info.Cell != req.Cell {
			continue
		}
		info := cloneSpokeInfo(entry.info)
		// Mark disconnected if heartbeat is stale.
		if now.Sub(entry.lastHeartbeat) > SpokeTimeout {
			info.State = hubv1.SpokeState_SPOKE_STATE_DISCONNECTED
		}
		spokes = append(spokes, info)
	}

	return &hubv1.ListSpokesResponse{Spokes: spokes}, nil
}

// ListCells aggregates registered spokes by cell. Spokes registered without
// a cell are grouped under the empty cell name.
func (s *Server) ListCells(_ context.Context, _ *hubv1.ListCellsRequest) (*hubv1.ListCellsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	byCell := make(map[string]*hubv1.CellInfo)
	now := time.Now()
	for _, entry := range s.spokes {
		cell := byCell[entry.info.Cell]
		if cell == nil {
			cell = &hubv1.CellInfo{Name: entry.info.Cell}
			byCell[entry.info.Cell] = cell
		}
		cell.TotalSpokes++
		if now.Sub(entry.lastHeartbeat) <= SpokeTimeout {
			cell.ConnectedSpokes++
		}
		cell.ActiveCaptures += int32(len(entry.captures))
		cell.ActiveReplays += activeReplayCount(entry.replays)
	}

	names := make([]string, 0, len(byCell))
	for name := range byCell {
		names = append(names, name)
	}
	sort.Strings(names)

	cells := make([]*hubv1.CellInfo, 0, len(names))
	for _, name := range names {
		cells = append(cells, byCell[name])
	}
	return &hubv1.ListCellsResponse{Cells: cells}, nil
}

// ListReplays returns replay shard statuses across spokes, optionally
// filtered by load test identity and cell.
func (s *Server) ListReplays(_ context.Context, req *hubv1.ListReplaysRequest) (*hubv1.ListReplaysResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var replays []*hubv1.ReplayInfo
	for spokeID, entry := range s.spokes {
		if req.GetCell() != "" && entry.info.Cell != req.Cell {
			continue
		}
		for _, rs := range entry.replays {
			if req.GetLoadTestName() != "" && rs.LoadTestName != req.LoadTestName {
				continue
			}
			if req.GetLoadTestNamespace() != "" && rs.LoadTestNamespace != req.LoadTestNamespace {
				continue
			}
			replays = append(replays, &hubv1.ReplayInfo{
				SpokeId: spokeID,
				Cell:    entry.info.Cell,
				Summary: rs,
			})
		}
	}

	return &hubv1.ListReplaysResponse{Replays: replays}, nil
}

// --- Status aggregation helpers ---

// ConnectedSpokeCount returns the number of connected spokes.
func (s *Server) ConnectedSpokeCount() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var count int32
	now := time.Now()
	for _, entry := range s.spokes {
		if now.Sub(entry.lastHeartbeat) <= SpokeTimeout {
			count++
		}
	}
	return count
}

// ActiveCaptureCount returns the total number of active captures across all spokes.
func (s *Server) ActiveCaptureCount() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var count int32
	for _, entry := range s.spokes {
		count += int32(len(entry.captures))
	}
	return count
}

// ActiveReplayCount returns the total number of non-terminal replay shards
// across all spokes.
func (s *Server) ActiveReplayCount() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var count int32
	for _, entry := range s.spokes {
		count += activeReplayCount(entry.replays)
	}
	return count
}

// SpokeStatuses returns a snapshot of spoke statuses for the CaptureHub CR status.
func (s *Server) SpokeStatuses() []SpokeStatusSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]SpokeStatusSnapshot, 0, len(s.spokes))
	for _, entry := range s.spokes {
		result = append(result, SpokeStatusSnapshot{
			Name:           entry.info.ClusterName,
			Cell:           entry.info.Cell,
			LastHeartbeat:  entry.lastHeartbeat,
			ActiveCaptures: entry.info.ActiveCaptures,
			ActiveReplays:  activeReplayCount(entry.replays),
		})
	}
	return result
}

// SpokeStatusSnapshot is a plain Go struct used by the controller to update
// CaptureHub CR status without importing proto types.
type SpokeStatusSnapshot struct {
	Name           string
	Cell           string
	LastHeartbeat  time.Time
	ActiveCaptures int32
	ActiveReplays  int32
}

// CellStatusSnapshot aggregates spoke state per cell for the CaptureHub CR
// status.
type CellStatusSnapshot struct {
	Name            string
	ConnectedSpokes int32
	TotalSpokes     int32
	ActiveCaptures  int32
	ActiveReplays   int32
}

// CellStatuses returns per-cell aggregates, sorted by cell name.
func (s *Server) CellStatuses() []CellStatusSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	byCell := make(map[string]*CellStatusSnapshot)
	now := time.Now()
	for _, entry := range s.spokes {
		cell := byCell[entry.info.Cell]
		if cell == nil {
			cell = &CellStatusSnapshot{Name: entry.info.Cell}
			byCell[entry.info.Cell] = cell
		}
		cell.TotalSpokes++
		if now.Sub(entry.lastHeartbeat) <= SpokeTimeout {
			cell.ConnectedSpokes++
		}
		cell.ActiveCaptures += int32(len(entry.captures))
		cell.ActiveReplays += activeReplayCount(entry.replays)
	}

	names := make([]string, 0, len(byCell))
	for name := range byCell {
		names = append(names, name)
	}
	sort.Strings(names)

	result := make([]CellStatusSnapshot, 0, len(names))
	for _, name := range names {
		result = append(result, *byCell[name])
	}
	return result
}

// SpokeSelection identifies a spoke chosen to run replay shards.
type SpokeSelection struct {
	SpokeID string
	Cell    string
}

// SelectSpokes returns connected spokes eligible for a load test. If cells
// is non-empty only spokes registered in one of those cells are eligible.
// If max > 0 at most max spokes are returned. The result is sorted by spoke
// ID so repeated planning is deterministic.
func (s *Server) SelectSpokes(cells []string, max int) []SpokeSelection {
	cellSet := make(map[string]bool, len(cells))
	for _, c := range cells {
		cellSet[c] = true
	}

	s.mu.RLock()
	now := time.Now()
	selected := make([]SpokeSelection, 0, len(s.spokes))
	for spokeID, entry := range s.spokes {
		if now.Sub(entry.lastHeartbeat) > SpokeTimeout {
			continue
		}
		if len(cellSet) > 0 && !cellSet[entry.info.Cell] {
			continue
		}
		selected = append(selected, SpokeSelection{SpokeID: spokeID, Cell: entry.info.Cell})
	}
	s.mu.RUnlock()

	sort.Slice(selected, func(i, j int) bool { return selected[i].SpokeID < selected[j].SpokeID })
	if max > 0 && len(selected) > max {
		selected = selected[:max]
	}
	return selected
}

// ReplayStatusSnapshot pairs a replay shard summary with its placement.
type ReplayStatusSnapshot struct {
	SpokeID string
	Cell    string
	Summary *hubv1.ReplayStatusSummary
}

// ReplayStatuses returns all shard statuses reported for one load test.
func (s *Server) ReplayStatuses(namespace, name string) []ReplayStatusSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []ReplayStatusSnapshot
	for spokeID, entry := range s.spokes {
		for _, rs := range entry.replays {
			if rs.LoadTestNamespace != namespace || rs.LoadTestName != name {
				continue
			}
			result = append(result, ReplayStatusSnapshot{
				SpokeID: spokeID,
				Cell:    entry.info.Cell,
				Summary: rs,
			})
		}
	}
	return result
}

// ClearReplayStatuses drops all shard statuses for one load test from the
// registry. Called by the coordinator when a load test is deleted so stale
// terminal statuses do not accumulate.
func (s *Server) ClearReplayStatuses(namespace, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range s.spokes {
		for key, rs := range entry.replays {
			if rs.LoadTestNamespace == namespace && rs.LoadTestName == name {
				delete(entry.replays, key)
			}
		}
		entry.info.ActiveReplays = activeReplayCount(entry.replays)
	}
}

// --- Helpers ---

func captureKey(namespace, name string) string {
	return namespace + "/" + name
}

func replayKey(rs *hubv1.ReplayStatusSummary) string {
	return rs.LoadTestNamespace + "/" + rs.LoadTestName + "/" + rs.ReplayName
}

// activeReplayCount counts non-terminal replay shards.
func activeReplayCount(replays map[string]*hubv1.ReplayStatusSummary) int32 {
	var count int32
	for _, rs := range replays {
		switch rs.Phase {
		case hubv1.ReplayPhase_REPLAY_PHASE_COMPLETED, hubv1.ReplayPhase_REPLAY_PHASE_FAILED:
		default:
			count++
		}
	}
	return count
}

func captureInfoFromSummary(cs *hubv1.CaptureStatusSummary, spokeID string) *hubv1.CaptureInfo {
	return &hubv1.CaptureInfo{
		CaptureName:      cs.CaptureName,
		CaptureNamespace: cs.CaptureNamespace,
		SpokeId:          spokeID,
		Phase:            cs.Phase,
		CapturedRequests: cs.CapturedRequests,
		BytesWritten:     cs.BytesWritten,
	}
}

func cloneSpokeInfo(info *hubv1.SpokeInfo) *hubv1.SpokeInfo {
	return &hubv1.SpokeInfo{
		SpokeId:        info.SpokeId,
		ClusterName:    info.ClusterName,
		ActiveCaptures: info.ActiveCaptures,
		LastHeartbeat:  info.LastHeartbeat,
		Capabilities:   info.Capabilities,
		State:          info.State,
		Cell:           info.Cell,
		ActiveReplays:  info.ActiveReplays,
	}
}
