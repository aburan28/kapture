package agent

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// CaptureMode controls whether the agent operates as a passive sink or an
// active reverse proxy.
type CaptureMode string

const (
	// CaptureModeMirror is the default mode where the agent acts as a passive
	// HTTP/gRPC sink that receives mirrored copies of traffic.
	CaptureModeMirror CaptureMode = "mirror"

	// CaptureModeProxy places the agent inline as a reverse proxy. Requests are
	// forwarded to an upstream target and both the request and response are
	// captured before the response is returned to the caller.
	CaptureModeProxy CaptureMode = "proxy"
)

// AgentServerConfig holds configuration for the AgentServer.
type AgentServerConfig struct {
	HTTPPort   int
	GRPCPort   int
	HealthPort int
	Handler    *CaptureHandler
	Logger     *slog.Logger

	// Mode selects mirror (default) or proxy capture mode.
	Mode CaptureMode

	// UpstreamHTTP is the base URL of the upstream HTTP service.
	// Required when Mode == CaptureModeProxy.
	UpstreamHTTP string

	// UpstreamGRPC is the host:port of the upstream gRPC service.
	// Required when Mode == CaptureModeProxy for gRPC traffic.
	UpstreamGRPC string

	// HealthChecker provides pre-flight/in-flight health checks.
	// Optional; when nil the legacy /healthz endpoint is used.
	HealthChecker *HealthChecker
}

// AgentServer hosts HTTP and gRPC sink servers that receive mirrored traffic,
// plus health and metrics endpoints.
type AgentServer struct {
	httpServer   *http.Server
	grpcServer   *grpc.Server
	healthServer *http.Server
	handler      *CaptureHandler
	log          *slog.Logger
	httpPort     int
	grpcPort     int
	healthPort   int

	mode          CaptureMode
	upstreamHTTP  string
	upstreamGRPC  string
	healthChecker *HealthChecker
}

// NewAgentServer creates a new AgentServer.
func NewAgentServer(cfg AgentServerConfig) *AgentServer {
	if cfg.HTTPPort <= 0 {
		cfg.HTTPPort = 8080
	}
	if cfg.GRPCPort <= 0 {
		cfg.GRPCPort = 9090
	}
	if cfg.HealthPort <= 0 {
		cfg.HealthPort = 8081
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Mode == "" {
		cfg.Mode = CaptureModeMirror
	}

	s := &AgentServer{
		handler:       cfg.Handler,
		log:           cfg.Logger,
		httpPort:      cfg.HTTPPort,
		grpcPort:      cfg.GRPCPort,
		healthPort:    cfg.HealthPort,
		mode:          cfg.Mode,
		upstreamHTTP:  cfg.UpstreamHTTP,
		upstreamGRPC:  cfg.UpstreamGRPC,
		healthChecker: cfg.HealthChecker,
	}

	// HTTP sink or proxy
	httpMux := http.NewServeMux()
	if cfg.Mode == CaptureModeProxy && cfg.UpstreamHTTP != "" {
		httpMux.HandleFunc("/", s.handleHTTPProxy)
	} else {
		httpMux.HandleFunc("/", s.handleHTTP)
	}
	s.httpServer = &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:           httpMux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// gRPC sink with unknown service handler
	if cfg.Mode == CaptureModeProxy && cfg.UpstreamGRPC != "" {
		s.grpcServer = grpc.NewServer(
			grpc.UnknownServiceHandler(s.handleGRPCProxy),
		)
	} else {
		s.grpcServer = grpc.NewServer(
			grpc.UnknownServiceHandler(s.handleGRPC),
		)
	}

	// Health + metrics
	healthMux := http.NewServeMux()
	if cfg.HealthChecker != nil {
		healthMux.HandleFunc("/healthz", s.handleHealth)
		healthMux.HandleFunc("/readyz", cfg.HealthChecker.HandleReadyz)
		healthMux.HandleFunc("/livez", cfg.HealthChecker.HandleLivez)
		healthMux.HandleFunc("/healthz/detail", cfg.HealthChecker.HandleHealthDetail)
	} else {
		healthMux.HandleFunc("/healthz", s.handleHealth)
	}
	healthMux.HandleFunc("/metrics", s.handleMetrics)
	s.healthServer = &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HealthPort),
		Handler:           healthMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s
}



// Start launches all servers and blocks until ctx is cancelled or an error occurs.
func (s *AgentServer) Start(ctx context.Context) error {
	errCh := make(chan error, 3)

	// gRPC listener
	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.grpcPort))
	if err != nil {
		return fmt.Errorf("listen gRPC port %d: %w", s.grpcPort, err)
	}

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		s.log.Info("starting HTTP sink server", "port", s.httpPort, "mode", s.mode)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()

	go func() {
		defer wg.Done()
		s.log.Info("starting gRPC sink server", "port", s.grpcPort, "mode", s.mode)
		if err := s.grpcServer.Serve(grpcLis); err != nil {
			errCh <- fmt.Errorf("grpc server: %w", err)
		}
	}()

	go func() {
		defer wg.Done()
		s.log.Info("starting health server", "port", s.healthPort)
		if err := s.healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("health server: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		return s.Shutdown()
	case err := <-errCh:
		_ = s.Shutdown()
		return err
	}
}

// Shutdown gracefully stops all servers.
func (s *AgentServer) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var errs []error
	s.grpcServer.GracefulStop()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("http shutdown: %w", err))
	}
	if err := s.healthServer.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("health shutdown: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("shutdown errors: %v", errs)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Mirror-mode handlers (original behaviour)
// ---------------------------------------------------------------------------

// handleHTTP is the catch-all HTTP handler for mirrored requests.
func (s *AgentServer) handleHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	captured := &CapturedHTTPRequest{
		Method:   r.Method,
		Path:     r.URL.Path,
		Headers:  r.Header,
		Body:     r.Body,
		Protocol: "HTTP",
		Metadata: map[string]string{
			"host":       r.Host,
			"remoteAddr": r.RemoteAddr,
			"query":      r.URL.RawQuery,
		},
	}

	if err := s.handler.Handle(r.Context(), captured); err != nil {
		s.log.Debug("capture failed", "error", err, "path", r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleGRPC is the grpc.UnknownServiceHandler that captures all mirrored gRPC calls.
func (s *AgentServer) handleGRPC(_ any, stream grpc.ServerStream) error {
	fullMethod, _ := grpc.MethodFromServerStream(stream)

	// Read the request message
	var msg rawMessage
	if err := stream.RecvMsg(&msg); err != nil && err != io.EOF {
		return status.Errorf(codes.Internal, "recv: %v", err)
	}

	// Extract gRPC metadata as headers
	headers := make(map[string][]string)
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		for k, v := range md {
			headers[k] = v
		}
	}

	captured := &CapturedHTTPRequest{
		Method:   "POST",
		Path:     fullMethod,
		Headers:  headers,
		Body:     bytes.NewReader(msg),
		Protocol: "gRPC",
		Metadata: map[string]string{
			"grpcMethod": fullMethod,
		},
	}

	if err := s.handler.Handle(stream.Context(), captured); err != nil {
		return status.Errorf(codes.Internal, "capture: %v", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Proxy-mode handlers (request + response capture)
// ---------------------------------------------------------------------------

// handleHTTPProxy acts as a reverse proxy: it forwards the request to the
// upstream, captures both the request and the response, and returns the
// upstream's response to the caller.
func (s *AgentServer) handleHTTPProxy(w http.ResponseWriter, r *http.Request) {
	upstream, err := url.Parse(s.upstreamHTTP)
	if err != nil {
		s.log.Error("invalid upstream URL", "upstream", s.upstreamHTTP, "error", err)
		w.WriteHeader(http.StatusBadGateway)
		return
	}

	// Buffer the incoming request body so we can capture it and still forward it.
	reqBodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.log.Warn("failed to read proxy request body", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	// Restore body for the reverse proxy.
	r.Body = io.NopCloser(bytes.NewReader(reqBodyBytes))

	// Build the captured request (we read the body above, so provide a reader).
	capturedReq := &CapturedHTTPRequest{
		Method:   r.Method,
		Path:     r.URL.Path,
		Headers:  r.Header.Clone(),
		Body:     bytes.NewReader(reqBodyBytes),
		Protocol: "HTTP",
		Metadata: map[string]string{
			"host":       r.Host,
			"remoteAddr": r.RemoteAddr,
			"query":      r.URL.RawQuery,
			"captureMode": "proxy",
		},
	}

	start := time.Now()

	// Use a response recorder to capture the upstream response.
	recorder := &proxyResponseRecorder{
		headers: make(http.Header),
		body:    &bytes.Buffer{},
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = upstream.Scheme
			req.URL.Host = upstream.Host
			req.Host = upstream.Host
		},
	}

	proxy.ServeHTTP(recorder, r)

	durationMs := time.Since(start).Milliseconds()

	capturedResp := &CapturedHTTPResponse{
		StatusCode: recorder.statusCode,
		Headers:    recorder.headers.Clone(),
		Body:       bytes.NewReader(recorder.body.Bytes()),
		DurationMs: durationMs,
		Metadata: map[string]string{
			"upstream": s.upstreamHTTP,
		},
	}

	// Write the response to the actual client.
	for k, vals := range recorder.headers {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	if recorder.statusCode > 0 {
		w.WriteHeader(recorder.statusCode)
	} else {
		w.WriteHeader(http.StatusBadGateway)
	}
	_, _ = w.Write(recorder.body.Bytes())

	// Capture request + response asynchronously to not block the caller.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.handler.HandleWithResponse(ctx, capturedReq, capturedResp); err != nil {
			s.log.Debug("proxy capture failed", "error", err, "path", r.URL.Path)
		}
	}()
}

// handleGRPCProxy forwards gRPC calls to the upstream and captures both the
// request message and the response message.
func (s *AgentServer) handleGRPCProxy(_ any, stream grpc.ServerStream) error {
	fullMethod, _ := grpc.MethodFromServerStream(stream)

	// Read incoming request message.
	var reqMsg rawMessage
	if err := stream.RecvMsg(&reqMsg); err != nil && err != io.EOF {
		return status.Errorf(codes.Internal, "recv: %v", err)
	}

	// Extract gRPC metadata as headers.
	headers := make(map[string][]string)
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		for k, v := range md {
			headers[k] = v
		}
	}

	capturedReq := &CapturedHTTPRequest{
		Method:   "POST",
		Path:     fullMethod,
		Headers:  headers,
		Body:     bytes.NewReader(reqMsg),
		Protocol: "gRPC",
		Metadata: map[string]string{
			"grpcMethod":  fullMethod,
			"captureMode": "proxy",
		},
	}

	// Dial upstream.
	start := time.Now()
	conn, err := grpc.NewClient(
		s.upstreamGRPC,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return status.Errorf(codes.Unavailable, "dial upstream: %v", err)
	}
	defer conn.Close()

	// Forward with the same metadata.
	ctx := stream.Context()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	var respMsg rawMessage
	err = conn.Invoke(ctx, fullMethod, &reqMsg, &respMsg)
	durationMs := time.Since(start).Milliseconds()

	capturedResp := &CapturedHTTPResponse{
		StatusCode: int(status.Code(err)),
		Headers:    make(map[string][]string),
		Body:       bytes.NewReader(respMsg),
		DurationMs: durationMs,
		Metadata: map[string]string{
			"upstream":  s.upstreamGRPC,
			"grpcCode":  status.Code(err).String(),
		},
	}

	// Capture regardless of upstream error.
	if captureErr := s.handler.HandleWithResponse(stream.Context(), capturedReq, capturedResp); captureErr != nil {
		s.log.Debug("gRPC proxy capture failed", "error", captureErr, "method", fullMethod)
	}

	if err != nil {
		return err
	}

	// Send response back to the caller.
	return stream.SendMsg(&respMsg)
}

// ---------------------------------------------------------------------------
// proxyResponseRecorder
// ---------------------------------------------------------------------------

// proxyResponseRecorder implements http.ResponseWriter to buffer the entire
// upstream response so it can be captured and then forwarded to the client.
type proxyResponseRecorder struct {
	headers    http.Header
	body       *bytes.Buffer
	statusCode int
}

func (r *proxyResponseRecorder) Header() http.Header        { return r.headers }
func (r *proxyResponseRecorder) WriteHeader(statusCode int)  { r.statusCode = statusCode }
func (r *proxyResponseRecorder) Write(b []byte) (int, error) { return r.body.Write(b) }

// rawMessage implements the encoding interfaces for raw gRPC bytes.
type rawMessage []byte

func (r *rawMessage) Marshal() ([]byte, error) { return *r, nil }
func (r *rawMessage) Unmarshal(b []byte) error  { *r = b; return nil }
func (r *rawMessage) ProtoMessage()             {}
func (r *rawMessage) Reset()                    { *r = nil }
func (r *rawMessage) String() string            { return string(*r) }

// ---------------------------------------------------------------------------
// Health & metrics endpoints
// ---------------------------------------------------------------------------

// handleHealth serves health check endpoint.
func (s *AgentServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleMetrics serves Prometheus-style metrics.
func (s *AgentServer) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	total, dropped, bytesRecv := s.handler.Metrics()
	responsesTotal := s.handler.ResponsesTotal()
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "# HELP capture_agent_requests_total Total captured requests\n")
	fmt.Fprintf(w, "# TYPE capture_agent_requests_total counter\n")
	fmt.Fprintf(w, "capture_agent_requests_total %d\n", total)
	fmt.Fprintf(w, "# HELP capture_agent_requests_dropped_total Dropped requests\n")
	fmt.Fprintf(w, "# TYPE capture_agent_requests_dropped_total counter\n")
	fmt.Fprintf(w, "capture_agent_requests_dropped_total %d\n", dropped)
	fmt.Fprintf(w, "# HELP capture_agent_bytes_received_total Bytes received\n")
	fmt.Fprintf(w, "# TYPE capture_agent_bytes_received_total counter\n")
	fmt.Fprintf(w, "capture_agent_bytes_received_total %d\n", bytesRecv)
	fmt.Fprintf(w, "# HELP capture_agent_responses_total Total captured responses\n")
	fmt.Fprintf(w, "# TYPE capture_agent_responses_total counter\n")
	fmt.Fprintf(w, "capture_agent_responses_total %d\n", responsesTotal)
}
