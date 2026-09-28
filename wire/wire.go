// Package wire implements a custom binary message format shared by the
// client and the server.
//
// Frame layout:
//
//	+--------+--------+---------+==========+===================+
//	| magic0 | magic1 | version | body   | fields ...        |
//	|        |        |         | length |                   |
//	+--------+--------+---------+==========+===================+
//
// body length is a uvarint counting the bytes of the field section, so
// a truncated or over-long frame is always detectable.
//
// Each field is a TLV triple:
//
//	field id : uvarint, must be > 0
//	wire type: 1 byte (WireVarint | WireBytes | WireString)
//	value    : uvarint                       (WireVarint)
//	         | uvarint length + raw bytes    (WireBytes / WireString)
//
// Compatibility contract:
//   - Decoders MUST accept frames of any known peer version and skip
//     fields whose id they do not understand, using the wire type to
//     find the field boundary.
//   - Encoders MUST NOT change the id, wire type, or byte layout of
//     fields that already exist in an older version.
//   - New fields are appended with fresh ids and are always optional:
//     a decoder that does not find them fills in the documented default.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	Magic0 byte = 0xC5
	Magic1 byte = 0xA1

	Version1 byte = 1
	Version2 byte = 2

	// maxVersion is the highest frame version this build understands.
	maxVersion = Version2
)

const (
	WireVarint byte = 0
	WireBytes  byte = 1
	WireString byte = 2
)

var (
	ErrBadMagic    = errors.New("wire: bad magic")
	ErrBadVersion  = errors.New("wire: unsupported version")
	ErrBadFieldID  = errors.New("wire: field id must be > 0")
	ErrBadWireType = errors.New("wire: unknown wire type")
	ErrTruncated   = errors.New("wire: truncated message")
	ErrTrailing    = errors.New("wire: trailing bytes after frame")
	ErrLength      = errors.New("wire: declared length exceeds remaining input")
	ErrVarint      = errors.New("wire: malformed varint")
)

// Field is a single decoded TLV field. Exactly one of Uvarint / Bytes is
// meaningful, selected by WireType.
type Field struct {
	ID       uint64
	WireType byte
	Uvarint  uint64
	Bytes    []byte
}

// reader is a bounds-checked cursor over a frame body. Every read path
// returns an error instead of panicking on malformed input.
type reader struct {
	buf []byte
	off int
}

func (r *reader) remaining() int { return len(r.buf) - r.off }

func (r *reader) byte() (byte, error) {
	if r.remaining() < 1 {
		return 0, ErrTruncated
	}
	b := r.buf[r.off]
	r.off++
	return b, nil
}

// uvarint reads a base-128 varint. binary.Uvarint consumes at most 10
// bytes and reports overflow, so a hostile "varint too long" input is
// rejected rather than looping or panicking.
func (r *reader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(r.buf[r.off:])
	switch {
	case n == 0:
		return 0, ErrTruncated
	case n < 0:
		return 0, ErrVarint
	}
	r.off += n
	return v, nil
}

func (r *reader) bytes(n uint64) ([]byte, error) {
	if n > uint64(r.remaining()) {
		return nil, fmt.Errorf("%w: want %d, have %d", ErrLength, n, r.remaining())
	}
	out := r.buf[r.off : r.off+int(n)]
	r.off += int(n)
	return out, nil
}

// encodeFrame wraps a field section in the frame preamble.
func encodeFrame(version byte, body []byte) []byte {
	dst := []byte{Magic0, Magic1, version}
	dst = binary.AppendUvarint(dst, uint64(len(body)))
	return append(dst, body...)
}

func appendVarintField(dst []byte, id uint64, v uint64) []byte {
	dst = binary.AppendUvarint(dst, id)
	dst = append(dst, WireVarint)
	return binary.AppendUvarint(dst, v)
}

func appendStringField(dst []byte, id uint64, s string) []byte {
	dst = binary.AppendUvarint(dst, id)
	dst = append(dst, WireString)
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// parse validates the frame header and body length, then decodes every
// field, known or not. Unknown fields are returned alongside known ones
// so callers can skip or preserve them. It never panics on malformed
// input.
func parse(data []byte) (version byte, fields []Field, err error) {
	r := &reader{buf: data}
	m0, err := r.byte()
	if err != nil {
		return 0, nil, err
	}
	m1, err := r.byte()
	if err != nil {
		return 0, nil, err
	}
	if m0 != Magic0 || m1 != Magic1 {
		return 0, nil, fmt.Errorf("%w: %#02x %#02x", ErrBadMagic, m0, m1)
	}
	version, err = r.byte()
	if err != nil {
		return 0, nil, err
	}
	if version < Version1 || version > maxVersion {
		return 0, nil, fmt.Errorf("%w: %d", ErrBadVersion, version)
	}
	n, err := r.uvarint()
	if err != nil {
		return 0, nil, err
	}
	if got := uint64(r.remaining()); got != n {
		if got < n {
			return 0, nil, fmt.Errorf("%w: body length %d, have %d", ErrTruncated, n, got)
		}
		return 0, nil, fmt.Errorf("%w: body length %d, have %d", ErrTrailing, n, got)
	}
	for r.remaining() > 0 {
		f := Field{}
		if f.ID, err = r.uvarint(); err != nil {
			return 0, nil, err
		}
		if f.ID == 0 {
			return 0, nil, ErrBadFieldID
		}
		if f.WireType, err = r.byte(); err != nil {
			return 0, nil, err
		}
		switch f.WireType {
		case WireVarint:
			if f.Uvarint, err = r.uvarint(); err != nil {
				return 0, nil, err
			}
		case WireBytes, WireString:
			var n uint64
			if n, err = r.uvarint(); err != nil {
				return 0, nil, err
			}
			if f.Bytes, err = r.bytes(n); err != nil {
				return 0, nil, err
			}
		default:
			return 0, nil, fmt.Errorf("%w: %d", ErrBadWireType, f.WireType)
		}
		fields = append(fields, f)
	}
	return version, fields, nil
}
