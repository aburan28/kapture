package backend

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/kapture-io/kapture/pkg/capture"
)

type Config struct {
	Endpoint   string
	Token      string
	HTTPClient *http.Client
}

type Client struct {
	base       *url.URL
	token      string
	http       *http.Client
	descriptor Descriptor
}

func ValidateEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("backend endpoint must be an HTTP(S) URL without credentials, query or fragment")
	}
	return nil
}

func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	if err := ValidateEndpoint(cfg.Endpoint); err != nil {
		return nil, err
	}
	for _, b := range []byte(cfg.Token) {
		if b <= 32 || b >= 127 {
			return nil, fmt.Errorf("backend token contains invalid characters")
		}
	}
	u, _ := url.Parse(cfg.Endpoint)
	hc := &http.Client{}
	if cfg.HTTPClient != nil {
		*hc = *cfg.HTTPClient
	} else {
		if base, ok := http.DefaultTransport.(*http.Transport); ok {
			transport := base.Clone()
			transport.ResponseHeaderTimeout = 30 * time.Second
			hc.Transport = transport
		} else {
			hc.Transport = http.DefaultTransport
		}
	}
	// A redirect must not turn a durable POST into a GET or forward credentials
	// to an endpoint other than the one the operator configured.
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c := &Client{base: u, token: cfg.Token, http: hc}
	describeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := c.request(describeCtx, http.MethodGet, "/v1/capabilities", nil, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&c.descriptor); err != nil {
		return nil, fmt.Errorf("decode backend descriptor: %w", err)
	}
	if c.descriptor.Version != Version || c.descriptor.Name == "" || len(c.descriptor.Capabilities) > 32 {
		return nil, fmt.Errorf("unsupported backend version or descriptor")
	}
	return c, nil
}

func (c *Client) Supports(cap Capability) bool { return c.descriptor.Supports(cap) }

func (c *Client) Require(cap Capability) error {
	if !c.Supports(cap) {
		return fmt.Errorf("backend does not support %s", cap)
	}
	return nil
}

func validPath(s string) bool {
	return s != "" && s != "." && s != ".." && len(s) <= 1024 && utf8.ValidString(s) && !strings.ContainsAny(s, "\\\x00") && !strings.HasPrefix(s, "/") && path.Clean(s) == s && !strings.HasPrefix(s, "../")
}

func encode(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

func capturePath(id string) (string, error) {
	if !validPath(id) {
		return "", fmt.Errorf("invalid capture identity")
	}
	return "/v1/captures/" + encode(id), nil
}

func (c *Client) request(ctx context.Context, method, suffix string, data []byte, contentType string) (*http.Response, error) {
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + suffix
	u.RawPath = ""
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Kapture-Backend-Version", Version)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("backend transport failed: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		// Response bodies may contain captured secrets echoed by a remote
		// service. They never become collector errors or Kubernetes status.
		return nil, fmt.Errorf("backend returned HTTP %d", resp.StatusCode)
	}
	return resp, nil
}

func (c *Client) PutRequest(ctx context.Context, id string, r *capture.Request) error {
	if err := c.Require(WriteRequests); err != nil {
		return err
	}
	p, err := capturePath(id)
	if err != nil {
		return err
	}
	if r == nil || r.ID == "" {
		return fmt.Errorf("request identity is required")
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(data) > MaxRecordBytes {
		return fmt.Errorf("request exceeds backend record limit")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := c.request(ctx, http.MethodPost, p+"/requests", data, "application/json")
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

func (c *Client) PutArtifact(ctx context.Context, id, name string, data []byte, typ string) (capture.Artifact, error) {
	if err := c.Require(WriteArtifacts); err != nil {
		return capture.Artifact{}, err
	}
	p, err := capturePath(id)
	if err != nil {
		return capture.Artifact{}, err
	}
	if !validPath(name) || len(data) > MaxArtifactBytes {
		return capture.Artifact{}, fmt.Errorf("invalid artifact path or size")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := c.request(ctx, http.MethodPut, p+"/artifacts/"+encode(name), data, typ)
	if err != nil {
		return capture.Artifact{}, err
	}
	defer resp.Body.Close()
	var result capture.Artifact
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return result, err
	}
	sum := sha256.Sum256(data)
	if result.CaptureID != id || result.Key == "" || result.SizeBytes != int64(len(data)) || result.SHA256 != hex.EncodeToString(sum[:]) {
		return result, fmt.Errorf("backend artifact acknowledgement does not match uploaded bytes")
	}
	return result, nil
}

func (c *Client) OpenReplay(ctx context.Context, opts capture.ReadOptions) (capture.Reader, error) {
	if err := c.Require(ReadRequests); err != nil {
		return nil, err
	}
	p, err := capturePath(opts.CaptureID)
	if err != nil {
		return nil, err
	}
	if opts.Limit < 0 || (!opts.StartTime.IsZero() && !opts.EndTime.IsZero() && !opts.StartTime.Before(opts.EndTime)) {
		return nil, fmt.Errorf("invalid replay selection")
	}
	data, err := json.Marshal(opts)
	if err != nil || len(data) > 8192 {
		return nil, fmt.Errorf("replay selection exceeds limit")
	}
	ctx, cancel := context.WithCancel(ctx)
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + p + "/requests"
	u.RawPath = ""
	u.RawQuery = url.Values{"options": []string{string(data)}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		cancel()
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Kapture-Backend-Version", Version)
	resp, err := c.http.Do(req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open backend replay stream: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("backend replay returned HTTP %d", resp.StatusCode)
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), MaxRecordBytes+1024)
	return &reader{body: resp.Body, scanner: scanner, cancel: cancel, opts: opts}, nil
}

type reader struct {
	body    io.ReadCloser
	scanner *bufio.Scanner
	cancel  context.CancelFunc
	opts    capture.ReadOptions
	seen    int64
	done    bool
}

func (r *reader) Next(ctx context.Context) (*capture.Request, error) {
	if r.done {
		return nil, io.EOF
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, r.cancel)
	defer stop()
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.ErrUnexpectedEOF
	}
	var f frame
	if err := json.Unmarshal(r.scanner.Bytes(), &f); err != nil {
		return nil, fmt.Errorf("invalid backend stream frame: %w", err)
	}
	if f.Error != "" {
		return nil, fmt.Errorf("backend replay stream failed")
	}
	if f.End {
		if f.Request != nil || f.Count != r.seen {
			return nil, fmt.Errorf("invalid backend terminal frame")
		}
		r.done = true
		_ = r.Close()
		return nil, io.EOF
	}
	if f.Request == nil || f.Request.ID == "" || !r.opts.Matches(f.Request) || (r.opts.Limit > 0 && r.seen >= r.opts.Limit) {
		return nil, fmt.Errorf("backend returned an invalid or unselected request")
	}
	r.seen++
	return f.Request, nil
}

func (r *reader) Close() error { r.cancel(); return r.body.Close() }
