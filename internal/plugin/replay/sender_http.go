package replay

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

// HTTPSenderConfig configures an HTTP-based replay Sender.
type HTTPSenderConfig struct {
	// TargetBaseURL is the base URL of the target service (e.g., "http://my-service:8080").
	TargetBaseURL string

	// Client is the HTTP client to use. If nil, a default client with sensible
	// timeouts is created.
	Client *http.Client

	// MaxResponseBody limits how many bytes of the response body are captured
	// in the Result. Defaults to 4KB.
	MaxResponseBody int64

	// OverrideHeaders are headers set on every replayed request, overriding
	// captured values. Useful for auth tokens, trace IDs, etc.
	OverrideHeaders map[string]string
}

// HTTPSender replays captured HTTP requests to a target service.
type HTTPSender struct {
	baseURL         string
	client          *http.Client
	maxResponseBody int64
	overrideHeaders map[string]string
}

// NewHTTPSender creates a Sender that replays via HTTP.
func NewHTTPSender(cfg HTTPSenderConfig) (*HTTPSender, error) {
	if cfg.TargetBaseURL == "" {
		return nil, fmt.Errorf("target base URL is required")
	}
	cfg.TargetBaseURL = strings.TrimRight(cfg.TargetBaseURL, "/")

	if cfg.Client == nil {
		cfg.Client = &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        200,
				MaxIdleConnsPerHost: 200,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	}
	if cfg.MaxResponseBody <= 0 {
		cfg.MaxResponseBody = 4 * 1024
	}

	return &HTTPSender{
		baseURL:         cfg.TargetBaseURL,
		client:          cfg.Client,
		maxResponseBody: cfg.MaxResponseBody,
		overrideHeaders: cfg.OverrideHeaders,
	}, nil
}

func (s *HTTPSender) Send(ctx context.Context, req *storage.CapturedRequest) (*Result, error) {
	url := s.baseURL + req.Path

	var body io.Reader
	if len(req.Body) > 0 {
		body = bytes.NewReader(req.Body)
	}

	httpReq, err := http.NewRequestWithContext(ctx, req.Method, url, body)
	if err != nil {
		return nil, fmt.Errorf("create http request: %w", err)
	}

	// Copy captured headers, skipping hop-by-hop headers that should not
	// be forwarded and Host (set by net/http from the URL).
	for k, vals := range req.Headers {
		if isHopByHopHeader(k) {
			continue
		}
		for _, v := range vals {
			httpReq.Header.Add(k, v)
		}
	}

	// Apply override headers.
	for k, v := range s.overrideHeaders {
		httpReq.Header.Set(k, v)
	}

	// Add replay metadata headers.
	httpReq.Header.Set("X-Kapture-Replay", "true")
	httpReq.Header.Set("X-Kapture-Request-ID", req.ID)

	start := time.Now()
	resp, err := s.client.Do(httpReq)
	duration := time.Since(start)

	if err != nil {
		// Return the error both in the Result and as the function error so the
		// engine's sendLoop correctly increments its error counter.
		return &Result{
			RequestID: req.ID,
			Duration:  duration,
			Error:     err,
			Timestamp: time.Now().UTC(),
		}, err
	}
	defer resp.Body.Close()

	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, s.maxResponseBody))

	result := &Result{
		RequestID:       req.ID,
		StatusCode:      resp.StatusCode,
		ResponseHeaders: resp.Header,
		ResponseBody:    respBody,
		Duration:        duration,
		Timestamp:       time.Now().UTC(),
	}
	if readErr != nil {
		result.Error = fmt.Errorf("read response body: %w", readErr)
	}

	return result, nil
}

func (s *HTTPSender) Close() error {
	s.client.CloseIdleConnections()
	return nil
}

// Verify HTTPSender implements Sender.
var _ Sender = (*HTTPSender)(nil)

// hopByHopHeaders are HTTP/1.1 hop-by-hop headers that must not be forwarded
// by proxies or replay engines. Host is also excluded because net/http sets it
// from the request URL, and Content-Length is best left for net/http to compute.
var hopByHopHeaders = map[string]bool{
	"Connection":       true,
	"Keep-Alive":       true,
	"Proxy-Connection": true,
	"Transfer-Encoding": true,
	"TE":               true,
	"Trailer":          true,
	"Upgrade":          true,
	"Host":             true,
	"Content-Length":   true,
}

func isHopByHopHeader(name string) bool {
	return hopByHopHeaders[http.CanonicalHeaderKey(name)]
}
