package l7

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/kapture-io/kapture/internal/agent"
	"github.com/kapture-io/kapture/internal/storage"
)

type ExportConfig struct {
	CaptureID      string
	Writer         storage.Writer
	Artifacts      storage.ArtifactWriter
	Filter         *agent.RequestFilter
	RedactHeaders  []string
	IncludeHeaders bool
	IncludeBody    bool
	PCAP           bool
	TriggerStatus  int
	History        time.Duration
	HistoryBytes   int
}
type retained struct {
	exchange *Exchange
	bytes    int
	saved    bool
}
type Exporter struct {
	cfg                            ExportConfig
	history                        []retained
	bytes                          int
	Exchanges, Requests, Evictions uint64
}

func NewExporter(cfg ExportConfig) (*Exporter, error) {
	if cfg.Writer == nil || cfg.Artifacts == nil || cfg.CaptureID == "" {
		return nil, fmt.Errorf("capture ID, request writer and artifact writer are required")
	}
	if cfg.RedactHeaders == nil {
		cfg.RedactHeaders = append([]string(nil), agent.DefaultRedactHeaders...)
	}
	if cfg.HistoryBytes <= 0 {
		cfg.HistoryBytes = 32 << 20
	}
	if cfg.History < 0 {
		return nil, fmt.Errorf("history duration must not be negative")
	}
	return &Exporter{cfg: cfg}, nil
}

func (e *Exporter) Accept(ctx context.Context, x *Exchange) error {
	if !e.cfg.Filter.Matches(&agent.CapturedHTTPRequest{Method: x.Request.Method, Path: x.Request.Path, Headers: x.Request.Headers, Protocol: x.Protocol}) {
		return nil
	}
	n := exchangeMemory(x)
	for len(e.history) > 0 && (e.bytes+n > e.cfg.HistoryBytes || (e.cfg.History > 0 && x.EndedAt.Sub(e.history[0].exchange.EndedAt) > e.cfg.History)) {
		e.bytes -= e.history[0].bytes
		e.history[0] = retained{}
		e.history = e.history[1:]
		e.Evictions++
	}
	trigger := e.cfg.TriggerStatus == 0 || x.Response.Status >= e.cfg.TriggerStatus
	if v := x.Response.Trailers.Get("Grpc-Status"); v != "" && v != "0" {
		trigger = true
	}
	if v := x.Response.Headers.Get("Grpc-Status"); v != "" && v != "0" {
		trigger = true
	}
	if n > e.cfg.HistoryBytes {
		if trigger {
			return e.persist(ctx, x)
		}
		e.Evictions++
		return nil
	}
	e.history = append(e.history, retained{exchange: x, bytes: n})
	e.bytes += n
	if trigger {
		for i := range e.history {
			v := &e.history[i]
			if !v.saved && v.exchange.Epoch == x.Epoch && v.exchange.Flow == x.Flow {
				if err := e.persist(ctx, v.exchange); err != nil {
					return err
				}
				v.saved = true
			}
		}
	}
	return nil
}
func (e *Exporter) MemoryBytes() int { return e.bytes }

func (e *Exporter) persist(ctx context.Context, original *Exchange) error {
	// Copy before applying policy: neither a retained observation nor a later
	// header filter may see a partially redacted mutable structure.
	data, err := json.Marshal(original)
	if err != nil {
		return err
	}
	var x Exchange
	if err = json.Unmarshal(data, &x); err != nil {
		return err
	}
	redactor := agent.NewHeaderRedactor(e.cfg.RedactHeaders)
	for _, m := range []*Message{&x.Request, &x.Response} {
		if redactor.Redact(m.Headers)+redactor.Redact(m.Trailers) > 0 {
			x.Quality.Redacted = true
		}
		if !e.cfg.IncludeHeaders {
			m.Headers = nil
			m.Trailers = nil
			x.Quality.Redacted = true
		}
		if !e.cfg.IncludeBody {
			m.Body = nil
			m.GRPC = nil
			m.BodyTruncated = m.BodyLength > 0
		}
	}
	for i := range x.HeaderProofs {
		p := &x.HeaderProofs[i]
		if len(e.cfg.RedactHeaders) > 0 || !e.cfg.IncludeHeaders {
			p.Block = nil
			p.Derived = true
		}
		for j := range p.Before.Entries {
			h := &p.Before.Entries[j]
			temp := http.Header{h.Name: []string{h.Value}}
			redactor.Redact(temp)
			h.Value = temp[h.Name][0]
		}
		if !e.cfg.IncludeHeaders {
			p.Before.Entries = nil
		}
	}
	data, err = json.Marshal(&x)
	if err != nil {
		return err
	}
	name := stableID(x.Epoch) + "/" + x.ID
	artifact, err := e.cfg.Artifacts.PutArtifact(ctx, e.cfg.CaptureID, name+".json", data, "application/json")
	if err != nil {
		return fmt.Errorf("write exchange evidence: %w", err)
	}
	if e.cfg.PCAP {
		p, err := SyntheticPCAP(&x)
		if err != nil {
			return err
		}
		if _, err = e.cfg.Artifacts.PutArtifact(ctx, e.cfg.CaptureID, name+".pcapng", p, "application/x-pcapng"); err != nil {
			return err
		}
	}
	e.Exchanges++
	if x.ReplayEligible() {
		protocol := "HTTP"
		metadata := map[string]string{"captureBackend": "EBPF", "l7Protocol": x.Protocol, "host": x.Request.Authority, "evidenceKey": artifact.Key, "evidenceSHA256": artifact.SHA256, "captureIntegrity": x.Quality.CaptureIntegrity, "packetAttribution": x.Quality.PacketAttribution}
		if len(x.Sources) > 0 {
			metadata["timestampBasis"] = x.Sources[0].TimestampBasis
		}
		if x.Quality.Redacted {
			metadata["redacted"] = "true"
		}
		body := x.Request.Body
		if x.Protocol == "gRPC" {
			protocol = "gRPC"
			body = x.Request.GRPC[0].Data
			metadata["grpcMethod"] = x.Request.Path
			metadata["grpcProjection"] = "single-message-observed"
		}
		r := &storage.CapturedRequest{ID: x.ID, Timestamp: x.StartedAt, Method: x.Request.Method, Path: x.Request.Path, Headers: x.Request.Headers, Body: body, ContentLength: int64(len(body)), Protocol: protocol, Metadata: metadata}
		if err = e.cfg.Writer.Write(ctx, r); err != nil {
			return fmt.Errorf("write replay projection: %w", err)
		}
		e.Requests++
	}
	return nil
}

func RedactionList(raw string) []string {
	if strings.EqualFold(strings.TrimSpace(raw), "none") {
		return []string{}
	}
	names := append([]string(nil), agent.DefaultRedactHeaders...)
	for _, s := range strings.Split(raw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			names = append(names, s)
		}
	}
	return names
}
