package wire

import (
	"errors"
	"testing"
)

// mustErr asserts that both decoders reject data without panicking.
func mustErr(t *testing.T, name string, data []byte) {
	t.Helper()
	if _, err := DecodeV1(data); err == nil {
		t.Errorf("%s: DecodeV1 accepted malformed input %x", name, data)
	}
	if _, err := DecodeV2(data); err == nil {
		t.Errorf("%s: DecodeV2 accepted malformed input %x", name, data)
	}
}

func TestMalformed(t *testing.T) {
	valid := V2{ID: 7, Name: "abc", Priority: 2}.Encode()

	// The frame header carries the body length, so truncation at ANY
	// byte offset is detectable and must be an error.
	for i := 0; i < len(valid); i++ {
		mustErr(t, "truncated", valid[:i])
	}

	mustErr(t, "empty", nil)
	mustErr(t, "bad magic", []byte{0x00, 0x00, 0x01})
	mustErr(t, "bad version", []byte{Magic0, Magic1, 0x7f, 0x00})
	mustErr(t, "trailing garbage", append(append([]byte{}, valid...), 0x00))
	// Field id 0 is reserved and must be rejected.
	mustErr(t, "field id zero", []byte{Magic0, Magic1, 0x01, 0x03, 0x00, WireVarint, 0x01})
	// Frame body length encoded as an over-long varint.
	mustErr(t, "length varint too long", append([]byte{Magic0, Magic1, 0x01},
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01))
	// Field value encoded as an over-long varint.
	mustErr(t, "value varint too long", append([]byte{Magic0, Magic1, 0x01, 0x0d, 0x01, WireVarint},
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01))
	// Declared string length far beyond the remaining body.
	mustErr(t, "length overflow", []byte{Magic0, Magic1, 0x01, 0x08, 0x02, WireString,
		0xff, 0xff, 0xff, 0xff, 0x0f, 'a'})
	mustErr(t, "unknown wire type", []byte{Magic0, Magic1, 0x01, 0x03, 0x01, 0x7e, 0x00})
}

func TestMalformedErrorsAreTyped(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want error
	}{
		{"field id zero", []byte{Magic0, Magic1, 0x01, 0x03, 0x00, WireVarint, 0x01}, ErrBadFieldID},
		{"length overflow", []byte{Magic0, Magic1, 0x01, 0x04, 0x02, WireString, 0x05, 'a'}, ErrLength},
		{"trailing", append(V1{ID: 1, Name: "x"}.Encode(), 0x00), ErrTrailing},
	}
	for _, c := range cases {
		if _, err := DecodeV1(c.data); !errors.Is(err, c.want) {
			t.Errorf("%s: want %v, got %v", c.name, c.want, err)
		}
	}
	valid := V2{ID: 7, Name: "abc", Priority: 2}.Encode()
	if _, err := DecodeV1(valid[:len(valid)-1]); !errors.Is(err, ErrTruncated) {
		t.Errorf("truncated: want ErrTruncated, got %v", err)
	}
}
