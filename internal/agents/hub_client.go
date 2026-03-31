package agents

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// HubClient manages the gRPC connection from an agent to the hub.
type HubClient struct {
	log        logr.Logger
	hubAddress string
	agentName  string
	clusterID  string
	agentID    string
	tlsConfig  *tls.Config

	conn   *grpc.ClientConn
	client hubv1.HubServiceClient

	mu            sync.RWMutex
	connected     bool
	heartbeatStop chan struct{}
	directiveStop chan struct{}

	// OnDirective is called when a hub-initiated directive is received.
	OnDirective func(directive *hubv1.WatchDirectivesResponse)
}

// HubClientConfig holds configuration for the HubClient.
type HubClientConfig struct {
	HubAddress string
	AgentName  string
	ClusterID  string
	TLSConfig  *tls.Config
	Logger     logr.Logger
}

// NewHubClient creates a new HubClient.
func NewHubClient(cfg HubClientConfig) *HubClient {
	return &HubClient{
		log:        cfg.Logger.WithName("hub-client"),
		hubAddress: cfg.HubAddress,
		agentName:  cfg.AgentName,
		clusterID:  cfg.ClusterID,
		tlsConfig:  cfg.TLSConfig,
	}
}

// Connect establishes the gRPC connection to the hub with retry logic.
func (c *HubClient) Connect(ctx context.Context) error {
	var opts []grpc.DialOption
	if c.tlsConfig != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(c.tlsConfig)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(c.hubAddress, opts...)
	if err != nil {
		return fmt.Errorf("dialing hub at %s: %w", c.hubAddress, err)
	}

	c.mu.Lock()
	c.conn = conn
	c.client = hubv1.NewHubServiceClient(conn)
	c.connected = true
	c.mu.Unlock()

	c.log.Info("connected to hub", "address", c.hubAddress)
	return nil
}

// Register registers this agent with the hub.
func (c *HubClient) Register(ctx context.Context) (int32, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()
	if client == nil {
		return 0, fmt.Errorf("not connected to hub")
	}

	resp, err := client.RegisterAgent(ctx, &hubv1.RegisterAgentRequest{
		AgentId:     c.agentName,
		ClusterName: c.clusterID,
	})
	if err != nil {
		return 0, fmt.Errorf("registering agent: %w", err)
	}

	if !resp.Accepted {
		return 0, fmt.Errorf("agent registration rejected: %s", resp.Message)
	}

	c.mu.Lock()
	c.agentID = c.agentName
	c.mu.Unlock()

	c.log.Info("registered with hub",
		"agentID", c.agentName,
		"heartbeatInterval", resp.HeartbeatIntervalSeconds,
	)

	return resp.HeartbeatIntervalSeconds, nil
}

// StartHeartbeat starts a background goroutine that sends periodic heartbeats.
func (c *HubClient) StartHeartbeat(ctx context.Context, interval time.Duration) {
	c.mu.Lock()
	if c.heartbeatStop != nil {
		close(c.heartbeatStop)
	}
	c.heartbeatStop = make(chan struct{})
	stopCh := c.heartbeatStop
	c.mu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-ticker.C:
				c.sendHeartbeat(ctx)
			}
		}
	}()

	c.log.Info("heartbeat started", "interval", interval)
}

func (c *HubClient) sendHeartbeat(ctx context.Context) {
	c.mu.RLock()
	client := c.client
	agentID := c.agentID
	c.mu.RUnlock()
	if client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err := client.Heartbeat(ctx, &hubv1.HeartbeatRequest{
		AgentId: agentID,
	})
	if err != nil {
		c.log.Error(err, "heartbeat failed")
		return
	}
}

// ReportStatus reports capture statuses to the hub.
func (c *HubClient) ReportStatus(ctx context.Context, statuses []*hubv1.CaptureStatusSummary) error {
	c.mu.RLock()
	client := c.client
	agentID := c.agentID
	c.mu.RUnlock()
	if client == nil {
		return fmt.Errorf("not connected to hub")
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, err := client.ReportCaptureStatus(ctx, &hubv1.ReportCaptureStatusRequest{
		AgentId:  agentID,
		Statuses: statuses,
	})
	if err != nil {
		return fmt.Errorf("reporting status: %w", err)
	}
	return nil
}

// WatchDirectives opens a server-streaming RPC to receive hub-initiated directives.
func (c *HubClient) WatchDirectives(ctx context.Context) {
	c.mu.Lock()
	if c.directiveStop != nil {
		close(c.directiveStop)
	}
	c.directiveStop = make(chan struct{})
	stopCh := c.directiveStop
	c.mu.Unlock()

	go c.watchDirectivesLoop(ctx, stopCh)
	c.log.Info("directive watcher started")
}

func (c *HubClient) watchDirectivesLoop(ctx context.Context, stopCh chan struct{}) {
	backoff := time.Second

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		default:
		}

		c.mu.RLock()
		client := c.client
		agentID := c.agentID
		c.mu.RUnlock()
		if client == nil {
			time.Sleep(backoff)
			continue
		}

		stream, err := client.WatchDirectives(ctx, &hubv1.WatchDirectivesRequest{
			AgentId: agentID,
		})
		if err != nil {
			c.log.Error(err, "opening directive stream", "backoff", backoff)
			time.Sleep(backoff)
			backoff = min(backoff*2, 60*time.Second)
			continue
		}

		backoff = time.Second
		for {
			resp, err := stream.Recv()
			if err != nil {
				c.log.Error(err, "directive stream closed")
				break
			}
			if c.OnDirective != nil {
				c.OnDirective(resp)
			}
		}
	}
}

// Deregister deregisters this agent from the hub.
func (c *HubClient) Deregister(ctx context.Context) error {
	c.mu.RLock()
	client := c.client
	agentID := c.agentID
	c.mu.RUnlock()
	if client == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err := client.DeregisterAgent(ctx, &hubv1.DeregisterAgentRequest{
		AgentId: agentID,
		Reason:  "agent shutting down",
	})
	if err != nil {
		c.log.Error(err, "failed to deregister from hub")
		return fmt.Errorf("deregistering agent: %w", err)
	}

	c.log.Info("deregistered from hub")
	return nil
}

// Close gracefully shuts down the hub client.
func (c *HubClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.heartbeatStop != nil {
		close(c.heartbeatStop)
		c.heartbeatStop = nil
	}
	if c.directiveStop != nil {
		close(c.directiveStop)
		c.directiveStop = nil
	}

	c.connected = false
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// IsConnected returns whether the client is connected to the hub.
func (c *HubClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}
