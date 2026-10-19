// Package backend exposes a versioned HTTP contract for independently deployed
// capture destinations. Backends need no access to Kapture internal packages.
package backend

import (
	"context"

	"github.com/kapture-io/kapture/pkg/capture"
)

const Version = "kapture.backend.v1"

type Capability string

const (
	WriteRequests    Capability = "requests.write"
	WriteArtifacts   Capability = "artifacts.write"
	ReadRequests     Capability = "requests.read"
	MaxRecordBytes              = 96 << 20
	MaxArtifactBytes            = 256 << 20
)

type Descriptor struct {
	Version      string       `json:"version"`
	Name         string       `json:"name"`
	Capabilities []Capability `json:"capabilities"`
}

func (d Descriptor) Supports(cap Capability) bool {
	for _, v := range d.Capabilities {
		if v == cap {
			return true
		}
	}
	return false
}

type Backend interface {
	Describe(context.Context) (Descriptor, error)
}

// RequestSink acknowledges only after accepting the complete redacted record
// durably. Request IDs are stable; implementations should deduplicate retries.
type RequestSink interface {
	PutRequest(context.Context, string, *capture.Request) error
}

// ArtifactSink stores exchange JSON, checkpoints, PCAPNG, and session metadata
// as opaque bytes; a backend need not understand each evidence format.
type ArtifactSink interface {
	PutArtifact(context.Context, string, string, []byte, string) (capture.Artifact, error)
}

type ReplaySource interface {
	OpenReplay(context.Context, capture.ReadOptions) (capture.Reader, error)
}

// A terminal frame distinguishes successful exhaustion from a broken connection.
// Each frame is one NDJSON line containing exactly one variant.
type frame struct {
	Request *capture.Request `json:"request,omitempty"`
	End     bool             `json:"end,omitempty"`
	Count   int64            `json:"count,omitempty"`
	Error   string           `json:"error,omitempty"`
}
