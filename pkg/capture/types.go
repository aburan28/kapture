// Package capture contains the stable data types shared by capture destinations
// and replay readers. It has no dependency on collectors or replay engines.
package capture

import (
	"context"
	"strings"
	"time"
)

// Request is the existing Kapture replay record. Bodies retain their wire bytes
// (base64 in JSON); metadata links richer exchange evidence when available.
type Request struct {
	ID            string              `json:"id"`
	Timestamp     time.Time           `json:"timestamp"`
	Method        string              `json:"method"`
	Path          string              `json:"path"`
	Headers       map[string][]string `json:"headers,omitempty"`
	Body          []byte              `json:"body,omitempty"`
	ContentLength int64               `json:"contentLength,omitempty"`
	Protocol      string              `json:"protocol"`
	Metadata      map[string]string   `json:"metadata,omitempty"`
}

// RequestURI preserves both current records containing a request target in Path
// and legacy Mirror records whose raw query is stored separately in metadata.
func (r *Request) RequestURI() string {
	if !strings.Contains(r.Path, "?") && r.Metadata["query"] != "" {
		return r.Path + "?" + r.Metadata["query"]
	}
	return r.Path
}

// Artifact describes an opaque evidence object. Payloads stay in the destination;
// this metadata can be indexed separately without copying payloads into SQL.
type Artifact struct {
	CaptureID        string    `json:"captureID"`
	CaptureNamespace string    `json:"captureNamespace,omitempty"`
	CaptureName      string    `json:"captureName,omitempty"`
	StorageBackend   string    `json:"storageBackend"`
	Bucket           string    `json:"bucket,omitempty"`
	Key              string    `json:"key"`
	Region           string    `json:"region,omitempty"`
	ContentType      string    `json:"contentType"`
	ContentEncoding  string    `json:"contentEncoding,omitempty"`
	SizeBytes        int64     `json:"sizeBytes"`
	SHA256           string    `json:"sha256"`
	CreatedAt        time.Time `json:"createdAt"`
}

type ReadOptions struct {
	CaptureID  string    `json:"captureID"`
	StartTime  time.Time `json:"startTime,omitempty"`
	EndTime    time.Time `json:"endTime,omitempty"`
	PathPrefix string    `json:"pathPrefix,omitempty"`
	Methods    []string  `json:"methods,omitempty"`
	Limit      int64     `json:"limit,omitempty"`
}

// Reader yields records incrementally and returns io.EOF only after a successful
// end of dataset. Next is sequential; Close releases a blocked network read.
type Reader interface {
	Next(context.Context) (*Request, error)
	Close() error
}

// Matches applies portable selection semantics. EndTime is exclusive.
func (o ReadOptions) Matches(r *Request) bool {
	if r == nil || (!o.StartTime.IsZero() && r.Timestamp.Before(o.StartTime)) || (!o.EndTime.IsZero() && !r.Timestamp.Before(o.EndTime)) || !strings.HasPrefix(r.Path, o.PathPrefix) {
		return false
	}
	if len(o.Methods) == 0 {
		return true
	}
	for _, m := range o.Methods {
		if r.Method == m {
			return true
		}
	}
	return false
}
