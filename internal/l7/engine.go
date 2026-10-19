package l7

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/http2"
)

type Config struct {
	MaxBodyBytes   int
	MaxBufferBytes int
	MaxMemoryBytes int
	MaxFlows       int
	MaxStreams     int
	IdleTimeout    time.Duration
}

func (c Config) defaults() Config {
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = 1 << 20
	}
	if c.MaxBufferBytes <= 0 {
		c.MaxBufferBytes = 4 << 20
	}
	if c.MaxMemoryBytes <= 0 {
		c.MaxMemoryBytes = 128 << 20
	}
	if c.MaxFlows <= 0 {
		c.MaxFlows = 256
	}
	if c.MaxStreams <= 0 {
		c.MaxStreams = 256
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 2 * time.Minute
	}
	return c
}

// Engine is single-owner: callers serialize observations, loss, expiry and close.
// Emission errors propagate to the caller so failed persistence cannot look like
// a successfully captured exchange.
type Engine struct {
	cfg          Config
	flows        map[string]*flow
	sequences    map[string]uint64
	emit         func(*Exchange) error
	serial       uint64
	taints       map[string][]string
	DecodeErrors uint64
	lastError    error
}
type flow struct {
	key, epoch, id, protocol, clientLane string
	lanes                                map[string]*lane
	requests                             []*Exchange
	streams                              map[uint32]*Exchange
	last                                 time.Time
	reasons                              []string
	verified                             bool
}
type lane struct {
	buf          []byte
	sources      []Source
	hpack        *headerDecoder
	block        []byte
	blockSources []Source
	blockStream  uint32
	blockEnd     bool
	prefaceSeen  bool
}

func NewEngine(cfg Config, emit func(*Exchange) error) *Engine {
	return &Engine{cfg: cfg.defaults(), flows: map[string]*flow{}, sequences: map[string]uint64{}, taints: map[string][]string{}, emit: emit}
}

type emissionError struct{ error }

func (e emissionError) Unwrap() error { return e.error }

func (e *Engine) Feed(o Observation) error {
	if o.Epoch == "" || o.Flow == "" || (o.Lane != "read" && o.Lane != "write") {
		return fmt.Errorf("epoch, connection and read/write lane are required")
	}
	if o.Timestamp.IsZero() {
		return fmt.Errorf("observation timestamp is required")
	}
	if len(o.Epoch) > 128 || len(o.Flow) > 1024 || len(o.ID) > 256 || o.OriginalLength < len(o.Data) {
		return fmt.Errorf("invalid observation identity or lengths")
	}
	if _, ok := e.sequences[o.Epoch]; !ok && len(e.sequences) >= e.cfg.MaxFlows {
		return fmt.Errorf("producer epoch limit exceeded")
	}
	if err := e.Expire(o.Timestamp); err != nil {
		return err
	}
	last := e.sequences[o.Epoch]
	if o.Sequence == 0 || (last != 0 && o.Sequence != last+1) || (last == 0 && o.Sequence != 1) {
		if err := e.Loss(o.Epoch, "producer_sequence_gap"); err != nil {
			return err
		}
	}
	e.sequences[o.Epoch] = o.Sequence
	key := o.Epoch + "/" + o.Flow
	f := e.flows[key]
	if f == nil {
		if len(e.flows) >= e.cfg.MaxFlows {
			if err := e.evictOldest("flow_limit"); err != nil {
				return err
			}
		}
		f = &flow{key: key, epoch: o.Epoch, id: o.Flow, lanes: map[string]*lane{}, streams: map[uint32]*Exchange{}, verified: o.Verified}
		f.reasons = append(f.reasons, e.taints[o.Epoch]...)
		if o.Sequence != 1 && last == 0 {
			f.reasons = append(f.reasons, "late_attach")
		}
		e.flows[key] = f
	}
	f.last = o.Timestamp
	f.verified = f.verified && o.Verified
	if o.Gap != "" {
		f.reasons = unique(f.reasons, o.Gap)
	}
	if o.OriginalLength > len(o.Data) {
		f.reasons = unique(f.reasons, "truncated_observation")
	}
	l := f.lanes[o.Lane]
	if l == nil {
		l = &lane{hpack: newHeaderDecoder()}
		f.lanes[o.Lane] = l
	}
	if len(o.Data) > e.cfg.MaxBufferBytes || len(l.buf)+len(o.Data) > e.cfg.MaxBufferBytes {
		return e.remove(f, "buffer_limit")
	}
	if len(o.Data) > 0 {
		l.buf = append(l.buf, o.Data...)
		l.sources = append(l.sources, Source{ID: o.ID, Sequence: o.Sequence, Lane: o.Lane, Length: len(o.Data), Timestamp: o.Timestamp, TimestampBasis: o.TimestampBasis})
	}
	if f.protocol == "" {
		detectProtocol(f)
	}
	var err error
	for pass := 0; pass < 2 && err == nil; pass++ {
		for _, name := range []string{"write", "read"} {
			if f.lanes[name] == nil {
				continue
			}
			if f.protocol == "HTTP/2" {
				err = e.parseH2(f, name)
			} else if f.protocol == "HTTP/1.1" {
				err = e.parseH1(f, name, o.Close)
			}
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		var output emissionError
		if errors.As(err, &output) {
			return err
		}
		e.DecodeErrors++
		e.lastError = err
		return e.remove(f, "decode_error")
	}
	if o.Close {
		return e.remove(f, "connection_closed")
	}
	for e.MemoryBytes() > e.cfg.MaxMemoryBytes {
		if err := e.evictOldest("memory_limit"); err != nil {
			return err
		}
	}
	return nil
}

func detectProtocol(f *flow) {
	for _, l := range f.lanes {
		b := l.buf
		if len(b) == 0 {
			continue
		}
		if bytes.HasPrefix([]byte(http2.ClientPreface), b) || bytes.HasPrefix(b, []byte(http2.ClientPreface)) {
			if len(b) >= len(http2.ClientPreface) {
				f.protocol = "HTTP/2"
			}
			continue
		}
		if len(b) >= 9 && b[3] == 4 && binary.BigEndian.Uint32(b[5:9]) == 0 {
			f.protocol = "HTTP/2"
			return
		}
		if bytes.HasPrefix(b, []byte("HTTP/1.")) {
			f.protocol = "HTTP/1.1"
			return
		}
		if i := bytes.IndexByte(b, ' '); i > 0 && i <= 16 {
			valid := true
			for _, c := range b[:i] {
				if c < 'A' || c > 'Z' {
					valid = false
				}
			}
			if valid {
				f.protocol = "HTTP/1.1"
				return
			}
		}
	}
}

func (l *lane) take(n int) []Source {
	var out []Source
	for n > 0 && len(l.sources) > 0 {
		s := &l.sources[0]
		k := min(n, s.Length)
		v := *s
		v.Length = k
		out = append(out, v)
		s.Offset += k
		s.Length -= k
		n -= k
		if s.Length == 0 {
			l.sources[0] = Source{}
			l.sources = l.sources[1:]
		}
	}
	consumed := sumLengths(out)
	l.buf = l.buf[consumed:]
	if len(l.buf) == 0 {
		l.buf = nil
	} else if consumed >= len(l.buf) {
		// A one-byte suffix must not pin a multi-megabyte consumed message.
		l.buf = append([]byte(nil), l.buf...)
	}
	return out
}
func sumLengths(s []Source) int {
	n := 0
	for _, v := range s {
		n += v.Length
	}
	return n
}

func (e *Engine) fresh(f *flow, stream uint32) *Exchange {
	e.serial++
	return &Exchange{FormatVersion: FormatVersion, ID: stableID(fmt.Sprintf("%s/%d", f.key, e.serial)), Epoch: f.epoch, Flow: f.id, Protocol: f.protocol, StreamID: stream, StartedAt: f.last, Quality: Quality{CaptureIntegrity: "unverified", Identity: "producer-connection", PacketAttribution: "unavailable"}}
}
func (e *Engine) finish(f *flow, x *Exchange, reason string) error {
	x.EndedAt = f.last
	x.Quality.Reasons = append(x.Quality.Reasons, f.reasons...)
	if reason != "" {
		x.Quality.Reasons = unique(x.Quality.Reasons, reason)
	}
	x.Quality.FramingComplete = x.Request.Complete && x.Response.Complete
	if f.verified {
		x.Quality.CaptureIntegrity = "verified"
	}
	if !f.verified {
		x.Quality.Identity = "epoch-pid-tuple-inferred"
	}
	if strings.HasPrefix(strings.ToLower(x.Request.Headers.Get("Content-Type")), "application/grpc") {
		x.Protocol = "gRPC"
		for _, m := range []*Message{&x.Request, &x.Response} {
			if !m.BodyTruncated && m.Complete {
				if err := grpcMessages(m); err != nil {
					x.Quality.Reasons = unique(x.Quality.Reasons, "grpc_framing_error")
				}
			}
		}
	}
	if e.emit != nil {
		if err := e.emit(x); err != nil {
			return emissionError{err}
		}
	}
	return nil
}
func grpcMessages(m *Message) error {
	b := m.Body
	for len(b) > 0 {
		if len(b) < 5 || b[0] > 1 {
			return fmt.Errorf("invalid gRPC envelope")
		}
		n := uint64(binary.BigEndian.Uint32(b[1:5]))
		if n > uint64(len(b)-5) {
			return io.ErrUnexpectedEOF
		}
		m.GRPC = append(m.GRPC, GRPCMessage{Compressed: b[0] == 1, Data: b[5 : 5+n]})
		b = b[5+n:]
	}
	return nil
}
func unique(a []string, s string) []string {
	for _, v := range a {
		if v == s {
			return a
		}
	}
	return append(a, s)
}

// Loss invalidates decoder state and flushes pending exchanges. It does not
// attempt to continue HPACK decoding after an unknown missing field block.
func (e *Engine) Loss(epoch, reason string) error {
	if epoch != "" {
		e.taints[epoch] = unique(e.taints[epoch], reason)
	} else {
		for key := range e.sequences {
			e.taints[key] = unique(e.taints[key], reason)
		}
	}
	for _, f := range e.flows {
		if epoch == "" || f.epoch == epoch {
			if err := e.remove(f, reason); err != nil {
				return err
			}
		}
	}
	return nil
}
func (e *Engine) EndEpoch(epoch string) error {
	err := e.Loss(epoch, "producer_stopped")
	delete(e.sequences, epoch)
	delete(e.taints, epoch)
	return err
}
func (e *Engine) remove(f *flow, reason string) error {
	delete(e.flows, f.key)
	switch reason {
	case "decode_error", "buffer_limit", "memory_limit", "flow_limit":
		// A tuple may reappear after eviction without a connection-generation
		// proof. Keep uncertainty for the bounded producer epoch.
		e.taints[f.epoch] = unique(e.taints[f.epoch], reason)
	}
	for _, x := range f.requests {
		if err := e.finish(f, x, reason); err != nil {
			return err
		}
	}
	ids := make([]int, 0, len(f.streams))
	for id := range f.streams {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	for _, id := range ids {
		if err := e.finish(f, f.streams[uint32(id)], reason); err != nil {
			return err
		}
	}
	return nil
}
func (e *Engine) evictOldest(reason string) error {
	var oldest *flow
	for _, f := range e.flows {
		if oldest == nil || f.last.Before(oldest.last) {
			oldest = f
		}
	}
	if oldest != nil {
		return e.remove(oldest, reason)
	}
	return nil
}
func (e *Engine) Expire(now time.Time) error {
	for _, f := range e.flows {
		if now.Sub(f.last) > e.cfg.IdleTimeout {
			if err := e.remove(f, "idle_expiry"); err != nil {
				return err
			}
		}
	}
	return nil
}
func (e *Engine) Close() error { return e.Loss("", "capture_stopped") }

func (e *Engine) MemoryBytes() int {
	n := 0
	for _, f := range e.flows {
		n += len(f.reasons)*64 + 256
		for _, l := range f.lanes {
			n += cap(l.buf) + cap(l.block) + len(l.sources)*128 + len(l.blockSources)*128
			for _, h := range l.hpack.state.Entries {
				n += len(h.Name) + len(h.Value) + 32
			}
		}
		for _, x := range f.requests {
			n += exchangeMemory(x)
		}
		for _, x := range f.streams {
			n += exchangeMemory(x)
		}
	}
	return n
}
func exchangeMemory(x *Exchange) int {
	n := len(x.Sources)*128 + 512
	for _, s := range x.Sources {
		n += len(s.ID) + len(s.Lane)
	}
	for _, m := range []Message{x.Request, x.Response} {
		n += cap(m.Body) + len(m.Path) + len(m.Authority) + cap(m.GRPC)*32
		for _, h := range []http.Header{m.Headers, m.Trailers} {
			for k, vs := range h {
				n += len(k)
				for _, v := range vs {
					n += len(v) + 32
				}
			}
		}
	}
	for _, p := range x.HeaderProofs {
		n += len(p.Block)
		for _, h := range p.Before.Entries {
			n += len(h.Name) + len(h.Value) + 32
		}
	}
	return n
}

func (e *Engine) parseH1(f *flow, name string, closed bool) error {
	l := f.lanes[name]
	for len(l.buf) > 0 {
		if bytes.Index(l.buf, []byte("\r\n\r\n")) < 0 {
			if len(l.buf) > 65536 {
				return fmt.Errorf("header limit")
			}
			return nil
		}
		if bytes.Index(l.buf, []byte("\r\n\r\n")) > 65536 {
			return fmt.Errorf("header limit")
		}
		r := bytes.NewReader(l.buf)
		br := bufio.NewReader(r)
		var m Message
		var body io.ReadCloser
		var req *http.Request
		response := bytes.HasPrefix(l.buf, []byte("HTTP/1."))
		if response {
			if len(f.requests) == 0 {
				return nil
			}
			req = &http.Request{Method: f.requests[0].Request.Method}
			resp, err := http.ReadResponse(br, req)
			if err != nil {
				return err
			}
			if resp.ContentLength < 0 && len(resp.TransferEncoding) == 0 && req.Method != "HEAD" && resp.StatusCode >= 200 && resp.StatusCode != 204 && resp.StatusCode != 304 && !closed {
				return nil
			}
			m = Message{Status: resp.StatusCode, Headers: resp.Header, Trailers: resp.Trailer}
			body = resp.Body
			if !bodyAvailable(l.buf, resp.ContentLength, resp.TransferEncoding, req.Method == "HEAD" || resp.StatusCode < 200 || resp.StatusCode == 204 || resp.StatusCode == 304, closed) {
				return nil
			}
		} else {
			parsed, err := http.ReadRequest(br)
			if err != nil {
				return err
			}
			req = parsed
			m = Message{Method: req.Method, Path: req.URL.RequestURI(), Authority: req.Host, Headers: req.Header, Trailers: req.Trailer}
			body = req.Body
			if !bodyAvailable(l.buf, req.ContentLength, req.TransferEncoding, false, closed) {
				return nil
			}
		}
		payload, err := io.ReadAll(io.LimitReader(body, int64(e.cfg.MaxBodyBytes)+1))
		var drained int64
		if err == nil {
			drained, err = io.Copy(io.Discard, body)
		}
		if err != nil {
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				return nil
			}
			return err
		}
		m.BodyLength = int64(len(payload)) + drained
		m.Body = payload[:min(len(payload), e.cfg.MaxBodyBytes)]
		m.BodyTruncated = m.BodyLength > int64(len(m.Body))
		m.Complete = true
		consumed := len(l.buf) - r.Len() - br.Buffered()
		if consumed <= 0 {
			return fmt.Errorf("HTTP parser made no progress")
		}
		sources := l.take(consumed)
		if response {
			x := f.requests[0]
			if m.Status < 200 && m.Status != 101 {
				x.Sources = append(x.Sources, sources...)
				continue
			}
			x.Response = m
			x.Sources = append(x.Sources, sources...)
			f.requests[0] = nil
			f.requests = f.requests[1:]
			if err := e.finish(f, x, ""); err != nil {
				return err
			}
			if m.Status == 101 || (x.Request.Method == "CONNECT" && m.Status >= 200 && m.Status < 300) {
				return fmt.Errorf("protocol tunnel unsupported")
			}
		} else {
			if len(f.requests) >= e.cfg.MaxStreams {
				return fmt.Errorf("pending request limit")
			}
			f.clientLane = name
			x := e.fresh(f, 0)
			x.Request = m
			x.Sources = sources
			if len(sources) > 0 {
				x.StartedAt = sources[0].Timestamp
			}
			f.requests = append(f.requests, x)
		}
	}
	return nil
}

// net/http reports malformed partial chunk trailers as syntax errors. Determine
// framing availability first so a read boundary cannot poison a valid message.
func bodyAvailable(data []byte, length int64, encoding []string, bodyless, closed bool) bool {
	if bodyless {
		return true
	}
	header := bytes.Index(data, []byte("\r\n\r\n")) + 4
	if header < 4 {
		return false
	}
	body := data[header:]
	if len(encoding) > 0 && encoding[0] == "chunked" {
		for {
			line := bytes.Index(body, []byte("\r\n"))
			if line < 0 {
				return false
			}
			size := strings.SplitN(string(body[:line]), ";", 2)[0]
			n, err := strconv.ParseUint(strings.TrimSpace(size), 16, 64)
			if err != nil {
				return true
			}
			body = body[line+2:]
			if n == 0 {
				return bytes.HasPrefix(body, []byte("\r\n")) || bytes.Contains(body, []byte("\r\n\r\n"))
			}
			if n > uint64(len(body)) || uint64(len(body))-n < 2 {
				return false
			}
			body = body[int(n)+2:]
		}
	}
	if length >= 0 {
		return int64(len(body)) >= length
	}
	return closed
}

func (e *Engine) parseH2(f *flow, name string) error {
	l := f.lanes[name]
	if bytes.HasPrefix([]byte(http2.ClientPreface), l.buf) && len(l.buf) < len(http2.ClientPreface) {
		return nil
	}
	if bytes.HasPrefix(l.buf, []byte(http2.ClientPreface)) {
		l.take(len(http2.ClientPreface))
		l.prefaceSeen = true
		f.clientLane = name
	}
	for len(l.buf) >= 9 {
		n := int(l.buf[0])<<16 | int(l.buf[1])<<8 | int(l.buf[2])
		if n > e.cfg.MaxBufferBytes-9 {
			return fmt.Errorf("HTTP/2 frame limit")
		}
		if len(l.buf) < n+9 {
			return nil
		}
		framer := http2.NewFramer(nil, bytes.NewReader(l.buf[:n+9]))
		// This framer reads one complete buffered frame. Cross-frame continuation
		// ordering is enforced below against lane.blockStream instead.
		framer.AllowIllegalReads = true
		framer.SetMaxReadFrameSize(uint32(e.cfg.MaxBufferBytes))
		frame, err := framer.ReadFrame()
		if err != nil {
			return err
		}
		refs := l.take(n + 9)
		if l.blockStream != 0 {
			if _, ok := frame.(*http2.ContinuationFrame); !ok {
				return fmt.Errorf("interleaved header block")
			}
		}
		switch v := frame.(type) {
		case *http2.HeadersFrame:
			l.block = append(l.block, v.HeaderBlockFragment()...)
			l.blockStream = v.StreamID
			l.blockEnd = v.StreamEnded()
			l.blockSources = append(l.blockSources, refs...)
			if v.HeadersEnded() {
				if err := e.headers(f, name); err != nil {
					return err
				}
			}
		case *http2.ContinuationFrame:
			if l.blockStream == 0 || l.blockStream != v.StreamID {
				return fmt.Errorf("unexpected continuation")
			}
			l.block = append(l.block, v.HeaderBlockFragment()...)
			l.blockSources = append(l.blockSources, refs...)
			if v.HeadersEnded() {
				if err := e.headers(f, name); err != nil {
					return err
				}
			}
		case *http2.DataFrame:
			x := f.streams[v.StreamID]
			if x == nil {
				return fmt.Errorf("DATA without known stream headers")
			}
			m := &x.Response
			if name == f.clientLane {
				m = &x.Request
			}
			if m.Complete || (m.Method == "" && m.Status == 0) {
				return fmt.Errorf("DATA outside an open message")
			}
			appendBody(m, v.Data(), e.cfg.MaxBodyBytes)
			x.Sources = append(x.Sources, refs...)
			if v.StreamEnded() {
				m.Complete = true
				if x.Request.Complete && x.Response.Complete {
					delete(f.streams, v.StreamID)
					if err := e.finish(f, x, ""); err != nil {
						return err
					}
				}
			}
		case *http2.RSTStreamFrame:
			if x := f.streams[v.StreamID]; x != nil {
				delete(f.streams, v.StreamID)
				if err := e.finish(f, x, "stream_reset"); err != nil {
					return err
				}
			}
		case *http2.PushPromiseFrame:
			return fmt.Errorf("server push is unsupported")
		case *http2.SettingsFrame:
			if !v.IsAck() {
				if err := v.ForeachSetting(func(s http2.Setting) error {
					if s.ID == http2.SettingHeaderTableSize && s.Val > 65536 {
						return fmt.Errorf("HPACK table limit")
					}
					return nil
				}); err != nil {
					return err
				}
			}
		}
		if len(l.block) > 65536 {
			return fmt.Errorf("header block limit")
		}
		if e.MemoryBytes() > e.cfg.MaxMemoryBytes {
			return fmt.Errorf("parser memory limit")
		}
	}
	return nil
}

func (e *Engine) headers(f *flow, name string) error {
	l := f.lanes[name]
	before := l.hpack.state.Clone()
	block := append([]byte(nil), l.block...)
	if len(block) > 65536 {
		return fmt.Errorf("header block limit")
	}
	fields, err := l.hpack.decode(l.block)
	if err != nil {
		return err
	}
	h := http.Header{}
	pseudo := map[string]string{}
	for _, v := range fields {
		if strings.HasPrefix(v.Name, ":") {
			if _, ok := pseudo[v.Name]; ok {
				return fmt.Errorf("duplicate pseudo-header")
			}
			pseudo[v.Name] = v.Value
		} else {
			h.Add(v.Name, v.Value)
		}
	}
	id := l.blockStream
	x := f.streams[id]
	if x == nil {
		if len(f.streams) >= e.cfg.MaxStreams {
			return fmt.Errorf("stream limit")
		}
		x = e.fresh(f, id)
		f.streams[id] = x
	}
	m := &x.Response
	if method := pseudo[":method"]; method != "" {
		if x.Request.Method != "" || pseudo[":status"] != "" || (f.clientLane != "" && f.clientLane != name) {
			return fmt.Errorf("duplicate request headers")
		}
		f.clientLane = name
		m = &x.Request
		m.Method = method
		m.Path = pseudo[":path"]
		m.Authority = pseudo[":authority"]
		m.Headers = h
	} else if status := pseudo[":status"]; status != "" {
		if name == f.clientLane || m.Status != 0 || m.Complete {
			return fmt.Errorf("response headers outside an open message")
		}
		v, err := strconv.Atoi(status)
		if err != nil || v < 100 || v > 599 {
			return fmt.Errorf("invalid HTTP/2 status")
		}
		if v < 200 {
			l.block = nil
			l.blockStream = 0
			l.blockSources = nil
			return nil
		}
		m.Status = v
		m.Headers = h
	} else {
		if name == f.clientLane {
			m = &x.Request
		}
		if len(pseudo) > 0 || m.Complete || (m.Method == "" && m.Status == 0) || !l.blockEnd {
			return fmt.Errorf("trailers outside an open message")
		}
		m.Trailers = h
	}
	x.Sources = append(x.Sources, l.blockSources...)
	x.HeaderProofs = append(x.HeaderProofs, HeaderProof{Lane: name, Before: before, Block: block})
	if len(x.Sources) > 0 {
		x.StartedAt = x.Sources[0].Timestamp
	}
	if l.blockEnd {
		m.Complete = true
	}
	l.block = nil
	l.blockStream = 0
	l.blockSources = nil
	l.blockEnd = false
	if x.Request.Complete && x.Response.Complete {
		delete(f.streams, id)
		return e.finish(f, x, "")
	}
	return nil
}
