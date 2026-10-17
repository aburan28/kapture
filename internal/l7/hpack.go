package l7

import (
	"bytes"
	"fmt"

	"golang.org/x/net/http2/hpack"
)

type HeaderField struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// HeaderState is the complete dynamic table immediately before a field block.
// Entries are newest first. Unlike a history prefix, its size is bounded by the
// HPACK table limit. Restore synthesizes an initialization block for a decoder.
type HeaderState struct {
	MaxBytes uint32        `json:"maxBytes"`
	Entries  []HeaderField `json:"entries,omitempty"`
}
type HeaderProof struct {
	Lane    string      `json:"lane"`
	Before  HeaderState `json:"before"`
	Block   []byte      `json:"block,omitempty"`
	Derived bool        `json:"derived,omitempty"`
}
type headerDecoder struct {
	decoder *hpack.Decoder
	state   HeaderState
}

func newHeaderDecoder() *headerDecoder {
	d := hpack.NewDecoder(4096, nil)
	d.SetAllowedMaxDynamicTableSize(65536)
	d.SetMaxStringLength(65536)
	return &headerDecoder{decoder: d, state: HeaderState{MaxBytes: 4096}}
}

func (s HeaderState) Clone() HeaderState {
	return HeaderState{MaxBytes: s.MaxBytes, Entries: append([]HeaderField(nil), s.Entries...)}
}
func (s *HeaderState) evict() {
	n := uint32(0)
	for i, h := range s.Entries {
		n += uint32(len(h.Name) + len(h.Value) + 32)
		if n > s.MaxBytes {
			clear(s.Entries[i:])
			s.Entries = s.Entries[:i]
			break
		}
	}
}

func (d *headerDecoder) decode(block []byte) ([]hpack.HeaderField, error) {
	var fields []hpack.HeaderField
	var limitErr error
	total := 0
	d.decoder.SetEmitEnabled(true)
	defer d.decoder.SetEmitFunc(nil)
	d.decoder.SetEmitFunc(func(f hpack.HeaderField) {
		total += len(f.Name) + len(f.Value)
		if total > 65536 || len(fields) >= 256 {
			limitErr = fmt.Errorf("header list or count limit exceeded")
			d.decoder.SetEmitEnabled(false)
			return
		}
		fields = append(fields, f)
	})
	if _, err := d.decoder.Write(block); err != nil {
		return nil, err
	}
	if err := d.decoder.Close(); err != nil {
		return nil, err
	}
	if limitErr != nil {
		return nil, limitErr
	}
	// The standard decoder validates the block. Walk the same representations
	// solely to mirror which emitted fields enter the dynamic table; Huffman and
	// static-table lookup remain the responsibility of x/net/hpack.
	index := 0
	for len(block) > 0 {
		b := block[0]
		if b&0x80 != 0 {
			_, rest, e := readInteger(block, 7)
			if e != nil {
				return nil, e
			}
			block = rest
			index++
			continue
		}
		if b&0xe0 == 0x20 {
			size, rest, e := readInteger(block, 5)
			if e != nil || size > 65536 {
				return nil, fmt.Errorf("invalid table size")
			}
			d.state.MaxBytes = uint32(size)
			d.state.evict()
			block = rest
			continue
		}
		prefix := uint8(4)
		insert := b&0xc0 == 0x40
		if insert {
			prefix = 6
		}
		name, rest, e := readInteger(block, prefix)
		if e != nil {
			return nil, e
		}
		block = rest
		if name == 0 {
			block, e = skipString(block)
			if e != nil {
				return nil, e
			}
		}
		block, e = skipString(block)
		if e != nil {
			return nil, e
		}
		if index >= len(fields) {
			return nil, fmt.Errorf("HPACK state mismatch")
		}
		if insert {
			h := fields[index]
			d.state.Entries = append([]HeaderField{{h.Name, h.Value}}, d.state.Entries...)
			d.state.evict()
		}
		index++
	}
	return fields, nil
}

func readInteger(b []byte, prefix uint8) (uint64, []byte, error) {
	if len(b) == 0 {
		return 0, nil, fmt.Errorf("short HPACK integer")
	}
	mask := byte((1 << prefix) - 1)
	n := uint64(b[0] & mask)
	b = b[1:]
	if n < uint64(mask) {
		return n, b, nil
	}
	for shift := uint(0); shift < 63; shift += 7 {
		if len(b) == 0 {
			return 0, nil, fmt.Errorf("short HPACK integer")
		}
		c := b[0]
		b = b[1:]
		n += uint64(c&127) << shift
		if c&128 == 0 {
			return n, b, nil
		}
	}
	return 0, nil, fmt.Errorf("HPACK integer overflow")
}
func skipString(b []byte) ([]byte, error) {
	n, rest, e := readInteger(b, 7)
	if e != nil || n > uint64(len(rest)) {
		return nil, fmt.Errorf("short HPACK string")
	}
	return rest[n:], nil
}
func encodeInteger(b *bytes.Buffer, n uint64, prefix uint8, flags byte) {
	mask := uint64((1 << prefix) - 1)
	if n < mask {
		b.WriteByte(flags | byte(n))
		return
	}
	b.WriteByte(flags | byte(mask))
	n -= mask
	for n >= 128 {
		b.WriteByte(byte(n&127) | 128)
		n >>= 7
	}
	b.WriteByte(byte(n))
}

// RestoreHeaderState creates a standard decoder without unsafe access to private
// fields. Oldest-first literal insertions reproduce exact dynamic indices.
func RestoreHeaderState(s HeaderState) (*hpack.Decoder, error) {
	if s.MaxBytes > 65536 || len(s.Entries) > 2048 {
		return nil, fmt.Errorf("invalid HPACK snapshot limits")
	}
	var block bytes.Buffer
	encodeInteger(&block, uint64(s.MaxBytes), 5, 0x20)
	total := 0
	for i := len(s.Entries) - 1; i >= 0; i-- {
		h := s.Entries[i]
		total += len(h.Name) + len(h.Value) + 32
		if total > int(s.MaxBytes) {
			return nil, fmt.Errorf("snapshot exceeds table size")
		}
		block.WriteByte(0x40)
		encodeInteger(&block, uint64(len(h.Name)), 7, 0)
		block.WriteString(h.Name)
		encodeInteger(&block, uint64(len(h.Value)), 7, 0)
		block.WriteString(h.Value)
	}
	d := hpack.NewDecoder(4096, nil)
	d.SetAllowedMaxDynamicTableSize(65536)
	d.SetMaxStringLength(65536)
	if _, err := d.DecodeFull(block.Bytes()); err != nil {
		return nil, err
	}
	return d, nil
}
