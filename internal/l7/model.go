// Package l7 reconstructs application exchanges from ordered plaintext observations.
// Capture integrity and protocol framing are deliberately separate properties.
package l7

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"time"
)

const FormatVersion = 1

// Observation is the adapter boundary. Flow identifies a connection within Epoch;
// Lane identifies a transport direction, not an HTTP/2 stream. Sequence is global
// to the producer, so a discontinuity affects all its active connections.
type Observation struct {
	Epoch          string    `json:"epoch"`
	Flow           string    `json:"flow"`
	Lane           string    `json:"lane"`
	Sequence       uint64    `json:"sequence"`
	ID             string    `json:"id"`
	Timestamp      time.Time `json:"timestamp"`
	TimestampBasis string    `json:"timestampBasis,omitempty"`
	Data           []byte    `json:"data,omitempty"`
	OriginalLength int       `json:"originalLength"`
	Gap            string    `json:"gap,omitempty"`
	Close          bool      `json:"close,omitempty"`
	// Verified is false for upstream eCaptureQ: it cannot prove kernel loss,
	// socket generations, or original API lengths before its payload cap.
	Verified bool `json:"verified"`
}

type Source struct {
	ID             string    `json:"id"`
	Sequence       uint64    `json:"sequence"`
	Lane           string    `json:"lane"`
	Length         int       `json:"length"`
	Offset         int       `json:"offset"`
	Timestamp      time.Time `json:"timestamp"`
	TimestampBasis string    `json:"timestampBasis,omitempty"`
}

type Message struct {
	Method        string        `json:"method,omitempty"`
	Path          string        `json:"path,omitempty"`
	Authority     string        `json:"authority,omitempty"`
	Status        int           `json:"status,omitempty"`
	Headers       http.Header   `json:"headers,omitempty"`
	Trailers      http.Header   `json:"trailers,omitempty"`
	Body          []byte        `json:"body,omitempty"`
	BodyLength    int64         `json:"bodyLength"`
	BodyTruncated bool          `json:"bodyTruncated,omitempty"`
	Complete      bool          `json:"complete"`
	GRPC          []GRPCMessage `json:"grpc,omitempty"`
}

// GRPCMessage preserves protobuf wire bytes; descriptors are not inferred.
type GRPCMessage struct {
	Compressed bool   `json:"compressed"`
	Data       []byte `json:"data"`
}

type Quality struct {
	FramingComplete   bool     `json:"framingComplete"`
	CaptureIntegrity  string   `json:"captureIntegrity"`
	Identity          string   `json:"identity"`
	PacketAttribution string   `json:"packetAttribution"`
	Reasons           []string `json:"reasons,omitempty"`
	Redacted          bool     `json:"redacted,omitempty"`
}

type Exchange struct {
	FormatVersion int           `json:"formatVersion"`
	ID            string        `json:"id"`
	Epoch         string        `json:"epoch"`
	Flow          string        `json:"flow"`
	Protocol      string        `json:"protocol"`
	StreamID      uint32        `json:"streamID,omitempty"`
	StartedAt     time.Time     `json:"startedAt"`
	EndedAt       time.Time     `json:"endedAt"`
	Request       Message       `json:"request"`
	Response      Message       `json:"response"`
	Sources       []Source      `json:"sources"`
	HeaderProofs  []HeaderProof `json:"headerProofs,omitempty"`
	Quality       Quality       `json:"quality"`
}

// ReplayEligible means the observed request is independently framed and has no
// known gaps. It does not upgrade an unverified capture to forensic certainty.
func (e *Exchange) ReplayEligible() bool {
	valid := e.Request.Complete && !e.Request.BodyTruncated && len(e.Quality.Reasons) == 0 && e.Request.Method != "" && e.Request.Path != ""
	if e.Protocol == "gRPC" {
		return valid && len(e.Request.GRPC) == 1 && !e.Request.GRPC[0].Compressed && len(e.Response.GRPC) <= 1
	}
	return valid
}

func stableID(s string) string { sum := sha256.Sum256([]byte(s)); return hex.EncodeToString(sum[:16]) }

func appendBody(m *Message, p []byte, max int) {
	m.BodyLength += int64(len(p))
	remaining := max - len(m.Body)
	if remaining > 0 {
		m.Body = append(m.Body, p[:min(len(p), remaining)]...)
	}
	m.BodyTruncated = int64(len(m.Body)) < m.BodyLength
}
