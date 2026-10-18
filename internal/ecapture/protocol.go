// Package ecapture consumes the pinned upstream eCaptureQ v1 protobuf protocol.
// Its public eCaptureQ publisher is independent of the unused generic output
// encoder. No terminal output is parsed into payload data.
package ecapture

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kapture-io/kapture/internal/l7"
	"google.golang.org/protobuf/encoding/protowire"
)

const Revision = "afcc9345ff8f09f96811a158cf768d284b4c7e1b"
const Patch = "kapture-openssl-ex-v1"
const MaxMessageBytes = 1 << 20

// Event mirrors only fields needed from upstream protobuf/proto/v1/ecaptureq.proto.
// Unknown fields are skipped with protobuf wire semantics for forward compatibility.
type Event struct {
	Timestamp                                                   int64
	UUID, SourceIP, DestinationIP, ProcessName, Direction       string
	SourcePort, DestinationPort, Length, OriginalLength, Format uint64
	PID                                                         int64
	Sequence                                                    uint64
	Payload                                                     []byte
}
type Entry struct {
	Event      *Event
	ProcessLog string
}

func Decode(data []byte) (Entry, error) {
	if len(data) > MaxMessageBytes {
		return Entry{}, fmt.Errorf("eCaptureQ message exceeds limit")
	}
	var entry Entry
	var typ uint64
	var raw []byte
	err := fields(data, func(n protowire.Number, t protowire.Type, b []byte, v uint64) error {
		switch n {
		case 1:
			if t != protowire.VarintType {
				return fmt.Errorf("invalid log type")
			}
			typ = v
		case 2:
			if t != protowire.BytesType {
				return fmt.Errorf("invalid event envelope")
			}
			raw = b
			entry.ProcessLog = ""
		case 3:
			if t != protowire.BytesType {
				return fmt.Errorf("invalid heartbeat envelope")
			}
			raw = nil
			entry.ProcessLog = ""
		case 4:
			if t != protowire.BytesType {
				return fmt.Errorf("invalid process log")
			}
			entry.ProcessLog = string(b)
			raw = nil
		}
		return nil
	})
	if err != nil {
		return entry, err
	}
	if typ != 2 {
		return entry, nil
	}
	if raw == nil {
		return entry, fmt.Errorf("event payload missing")
	}
	e := &Event{}
	err = fields(raw, func(n protowire.Number, t protowire.Type, b []byte, v uint64) error {
		isBytes := n == 2 || n == 3 || n == 5 || n == 8 || n == 11 || n == 14 || n == 16
		if n >= 1 && n <= 17 {
			if (isBytes && t != protowire.BytesType) || (!isBytes && t != protowire.VarintType) {
				return fmt.Errorf("invalid event field %d", n)
			}
		}
		switch n {
		case 1:
			e.Timestamp = int64(v)
		case 2:
			e.UUID = string(b)
		case 3:
			e.SourceIP = string(b)
		case 4:
			e.SourcePort = v
		case 5:
			e.DestinationIP = string(b)
		case 6:
			e.DestinationPort = v
		case 7:
			e.PID = int64(v)
		case 8:
			e.ProcessName = string(b)
		case 10:
			e.Length = v
		case 11:
			e.Payload = append([]byte(nil), b...)
		case 12:
			e.Format = v
		case 14:
			e.Direction = string(b)
		case 15:
			e.OriginalLength = v
		case 17:
			e.Sequence = v
		}
		return nil
	})
	if err != nil {
		return entry, err
	}
	if e.Length != uint64(len(e.Payload)) || e.OriginalLength < e.Length || e.SourcePort > 65535 || e.DestinationPort > 65535 {
		return entry, fmt.Errorf("invalid event lengths or ports")
	}
	entry.Event = e
	return entry, nil
}

func fields(data []byte, visit func(protowire.Number, protowire.Type, []byte, uint64) error) error {
	for len(data) > 0 {
		n, t, k := protowire.ConsumeTag(data)
		if k < 0 {
			return protowire.ParseError(k)
		}
		data = data[k:]
		var b []byte
		var v uint64
		switch t {
		case protowire.BytesType:
			b, k = protowire.ConsumeBytes(data)
		case protowire.VarintType:
			v, k = protowire.ConsumeVarint(data)
		default:
			k = protowire.ConsumeFieldValue(n, t, data)
		}
		if k < 0 {
			return protowire.ParseError(k)
		}
		if err := visit(n, t, b, v); err != nil {
			return err
		}
		data = data[k:]
	}
	return nil
}

func (e *Event) Observation(epoch string, now time.Time) (l7.Observation, error) {
	if e.Format != 1 {
		return l7.Observation{}, fmt.Errorf("plaintext text-format events are required")
	}
	if e.PID <= 0 || net.ParseIP(e.SourceIP) == nil || net.ParseIP(e.DestinationIP) == nil || e.SourcePort == 0 || e.DestinationPort == 0 {
		return l7.Observation{}, fmt.Errorf("unattributed connection")
	}
	if e.Direction != "read" && e.Direction != "write" {
		return l7.Observation{}, fmt.Errorf("unknown direction")
	}
	endpoints := []string{net.JoinHostPort(e.SourceIP, strconv.FormatUint(e.SourcePort, 10)), net.JoinHostPort(e.DestinationIP, strconv.FormatUint(e.DestinationPort, 10))}
	sort.Strings(endpoints)
	o := l7.Observation{Epoch: epoch, Flow: fmt.Sprintf("%d/%s/%s", e.PID, endpoints[0], endpoints[1]), Lane: e.Direction, Sequence: e.Sequence, ID: e.UUID, Timestamp: now, Data: e.Payload, OriginalLength: int(e.OriginalLength)}
	o.TimestampBasis = "collector-receive"
	// The pinned OpenSSL ABI clamps data_len before publication. Equality at
	// its cap cannot distinguish a complete call from a lost tail.
	if len(e.Payload) >= 16384 {
		o.Gap = "upstream_payload_cap"
	}
	// The upstream publisher's timestamp may be derived from a monotonic
	// value. Use explicit collector receive time rather than invent UTC.
	if e.Timestamp > 946684800 && e.Timestamp < now.Add(time.Hour).Unix() {
		o.Timestamp = time.Unix(e.Timestamp, 0)
		o.TimestampBasis = "producer-utc-seconds"
	}
	return o, nil
}

func IsLossLog(s string) bool {
	s = strings.ToLower(s)
	return strings.Contains(s, "lost_samples") || strings.Contains(s, "lost samples") || strings.Contains(s, "samples lost") || strings.Contains(s, "backpressure") || strings.Contains(s, "decode failed") || strings.Contains(s, "failed to decode")
}
