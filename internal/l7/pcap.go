package l7

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// SyntheticPCAP creates a standalone, explicitly reconstructed plaintext stream.
// It does not reuse original TLS/TCP sequence numbers or claim wire fidelity.
// HTTP/2 and gRPC get a fresh compression context and stream 1 so a selected
// exchange remains decodable without earlier traffic on its original connection.
func SyntheticPCAP(x *Exchange) ([]byte, error) {
	var request, response []byte
	if x.Protocol == "HTTP/2" || x.Protocol == "gRPC" {
		var err error
		request, response, err = h2Projection(x)
		if err != nil {
			return nil, err
		}
	} else {
		request = h1Projection(x.Request, true)
		response = h1Projection(x.Response, false)
	}
	var out bytes.Buffer
	block := func(typ uint32, body []byte) {
		size := uint32(len(body) + 12)
		binary.Write(&out, binary.LittleEndian, typ)
		binary.Write(&out, binary.LittleEndian, size)
		out.Write(body)
		binary.Write(&out, binary.LittleEndian, size)
	}
	var section bytes.Buffer
	binary.Write(&section, binary.LittleEndian, uint32(0x1a2b3c4d))
	binary.Write(&section, binary.LittleEndian, uint16(1))
	binary.Write(&section, binary.LittleEndian, uint16(0))
	binary.Write(&section, binary.LittleEndian, uint64(^uint64(0)))
	block(0x0a0d0d0a, section.Bytes())
	var iface bytes.Buffer
	binary.Write(&iface, binary.LittleEndian, uint16(101))
	binary.Write(&iface, binary.LittleEndian, uint16(0))
	binary.Write(&iface, binary.LittleEndian, uint32(65535))
	option := func(code uint16, s string) {
		binary.Write(&iface, binary.LittleEndian, code)
		binary.Write(&iface, binary.LittleEndian, uint16(len(s)))
		iface.WriteString(s)
		for iface.Len()%4 != 0 {
			iface.WriteByte(0)
		}
	}
	option(2, "kapture-reconstructed")
	option(1, "Synthetic plaintext; not a wire capture. Exchange "+x.ID+" original protocol "+x.Protocol+" stream "+strconv.FormatUint(uint64(x.StreamID), 10))
	binary.Write(&iface, binary.LittleEndian, uint32(0))
	block(1, iface.Bytes())
	timestamp := x.StartedAt
	if timestamp.IsZero() {
		timestamp = time.Unix(0, 0)
	}
	tick := int64(0)
	packet := func(client bool, seq, ack uint32, flags byte, payload []byte) {
		p := tcpPacket(client, seq, ack, flags, payload)
		var b bytes.Buffer
		us := uint64(timestamp.UnixMicro() + tick)
		tick++
		for _, v := range []uint32{0, uint32(us >> 32), uint32(us), uint32(len(p)), uint32(len(p))} {
			binary.Write(&b, binary.LittleEndian, v)
		}
		b.Write(p)
		for b.Len()%4 != 0 {
			b.WriteByte(0)
		}
		block(6, b.Bytes())
	}
	packet(true, 1, 0, 2, nil)
	packet(false, 1, 2, 0x12, nil)
	packet(true, 2, 2, 0x10, nil)
	clientSeq, serverSeq := uint32(2), uint32(2)
	for len(request) > 0 {
		n := min(16384, len(request))
		packet(true, clientSeq, serverSeq, 0x18, request[:n])
		clientSeq += uint32(n)
		request = request[n:]
	}
	packet(false, serverSeq, clientSeq, 0x10, nil)
	for len(response) > 0 {
		n := min(16384, len(response))
		packet(false, serverSeq, clientSeq, 0x18, response[:n])
		serverSeq += uint32(n)
		response = response[n:]
	}
	packet(true, clientSeq, serverSeq, 0x11, nil)
	clientSeq++
	packet(false, serverSeq, clientSeq, 0x11, nil)
	packet(true, clientSeq, serverSeq+1, 0x10, nil)
	return out.Bytes(), nil
}

func h1Projection(m Message, request bool) []byte {
	var b bytes.Buffer
	if request {
		fmt.Fprintf(&b, "%s %s HTTP/1.1\r\n", m.Method, m.Path)
		if m.Authority != "" {
			fmt.Fprintf(&b, "Host: %s\r\n", m.Authority)
		}
	} else {
		status := m.Status
		if status == 0 {
			status = 599
		}
		fmt.Fprintf(&b, "HTTP/1.1 %d %s\r\n", status, http.StatusText(status))
	}
	keys := make([]string, 0, len(m.Headers))
	for k := range m.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if strings.HasPrefix(k, ":") || strings.EqualFold(k, "content-length") || strings.EqualFold(k, "transfer-encoding") || strings.EqualFold(k, "connection") {
			continue
		}
		for _, v := range m.Headers[k] {
			if !strings.ContainsAny(k+v, "\r\n") {
				fmt.Fprintf(&b, "%s: %s\r\n", k, v)
			}
		}
	}
	fmt.Fprintf(&b, "Content-Length: %d\r\nX-Kapture-Reconstructed: true\r\nConnection: close\r\n\r\n", len(m.Body))
	b.Write(m.Body)
	return b.Bytes()
}

func h2Projection(x *Exchange) ([]byte, []byte, error) {
	var req, resp bytes.Buffer
	req.WriteString(http2.ClientPreface)
	for _, part := range []struct {
		out     *bytes.Buffer
		m       Message
		request bool
	}{{&req, x.Request, true}, {&resp, x.Response, false}} {
		fr := http2.NewFramer(part.out, nil)
		if err := fr.WriteSettings(); err != nil {
			return nil, nil, err
		}
		var encoded bytes.Buffer
		enc := hpack.NewEncoder(&encoded)
		write := func(fields []hpack.HeaderField, end bool) error {
			encoded.Reset()
			for _, v := range fields {
				if err := enc.WriteField(v); err != nil {
					return err
				}
			}
			block := encoded.Bytes()
			first := true
			for first || len(block) > 0 {
				n := min(16384, len(block))
				var err error
				if first {
					err = fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block[:n], EndStream: end, EndHeaders: n == len(block)})
					first = false
				} else {
					err = fr.WriteContinuation(1, n == len(block), block[:n])
				}
				if err != nil {
					return err
				}
				block = block[n:]
			}
			return nil
		}
		fields := []hpack.HeaderField{}
		if part.request {
			fields = append(fields, hpack.HeaderField{Name: ":method", Value: part.m.Method}, hpack.HeaderField{Name: ":path", Value: part.m.Path}, hpack.HeaderField{Name: ":scheme", Value: "https"}, hpack.HeaderField{Name: ":authority", Value: part.m.Authority})
		} else {
			status := part.m.Status
			if status == 0 {
				status = 599
			}
			fields = append(fields, hpack.HeaderField{Name: ":status", Value: strconv.Itoa(status)})
		}
		add := func(h http.Header) []hpack.HeaderField {
			var out []hpack.HeaderField
			keys := make([]string, 0, len(h))
			for k := range h {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if strings.HasPrefix(k, ":") || strings.EqualFold(k, "content-length") || strings.EqualFold(k, "connection") || strings.EqualFold(k, "transfer-encoding") {
					continue
				}
				for _, v := range h[k] {
					if !strings.ContainsAny(k+v, "\r\n") {
						out = append(out, hpack.HeaderField{Name: strings.ToLower(k), Value: v})
					}
				}
			}
			return out
		}
		fields = append(fields, add(part.m.Headers)...)
		fields = append(fields, hpack.HeaderField{Name: "x-kapture-reconstructed", Value: "true"})
		end := len(part.m.Body) == 0 && len(part.m.Trailers) == 0
		if err := write(fields, end); err != nil {
			return nil, nil, err
		}
		body := part.m.Body
		for len(body) > 0 {
			n := min(16384, len(body))
			if err := fr.WriteData(1, n == len(body) && len(part.m.Trailers) == 0, body[:n]); err != nil {
				return nil, nil, err
			}
			body = body[n:]
		}
		if len(part.m.Trailers) > 0 {
			if err := write(add(part.m.Trailers), true); err != nil {
				return nil, nil, err
			}
		}
	}
	return req.Bytes(), resp.Bytes(), nil
}

func tcpPacket(client bool, seq, ack uint32, flags byte, payload []byte) []byte {
	p := make([]byte, 40+len(payload))
	p[0] = 0x45
	binary.BigEndian.PutUint16(p[2:4], uint16(len(p)))
	p[8] = 64
	p[9] = 6
	src, dst := []byte{192, 0, 2, 1}, []byte{192, 0, 2, 2}
	sport, dport := uint16(49152), uint16(80)
	if !client {
		src, dst = dst, src
		sport, dport = dport, sport
	}
	copy(p[12:16], src)
	copy(p[16:20], dst)
	binary.BigEndian.PutUint16(p[20:22], sport)
	binary.BigEndian.PutUint16(p[22:24], dport)
	binary.BigEndian.PutUint32(p[24:28], seq)
	binary.BigEndian.PutUint32(p[28:32], ack)
	p[32] = 0x50
	p[33] = flags
	binary.BigEndian.PutUint16(p[34:36], 65535)
	copy(p[40:], payload)
	binary.BigEndian.PutUint16(p[10:12], checksum(p[:20]))
	pseudo := append([]byte(nil), p[12:20]...)
	pseudo = append(pseudo, 0, 6, byte((len(p)-20)>>8), byte(len(p)-20))
	pseudo = append(pseudo, p[20:]...)
	binary.BigEndian.PutUint16(p[36:38], checksum(pseudo))
	return p
}
func checksum(p []byte) uint16 {
	n := uint32(0)
	for len(p) >= 2 {
		n += uint32(binary.BigEndian.Uint16(p))
		p = p[2:]
	}
	if len(p) > 0 {
		n += uint32(p[0]) << 8
	}
	for n>>16 != 0 {
		n = (n & 65535) + (n >> 16)
	}
	return ^uint16(n)
}
