package wire

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestGoldenV1 pins the v1 wire format byte-for-byte against sample
// frames stored in the repository. The samples were constructed by
// hand, independently of the encoder, so a layout change in Encode
// fails here.
func TestGoldenV1(t *testing.T) {
	samples := []struct {
		file string
		msg  V1
	}{
		{"id1_hello.bin", V1{ID: 1, Name: "hello"}},
		{"id300_empty.bin", V1{ID: 300, Name: ""}},
		{"id0_x.bin", V1{ID: 0, Name: "x"}},
	}
	for _, s := range samples {
		t.Run(s.file, func(t *testing.T) {
			golden, err := os.ReadFile(filepath.Join("..", "testdata", "v1", s.file))
			if err != nil {
				t.Fatal(err)
			}
			if got := s.msg.Encode(); !bytes.Equal(got, golden) {
				t.Errorf("Encode() = %x, golden = %x", got, golden)
			}
			dec, err := DecodeV1(golden)
			if err != nil {
				t.Fatalf("DecodeV1(golden): %v", err)
			}
			if dec.ID != s.msg.ID || dec.Name != s.msg.Name {
				t.Errorf("decoded %+v, want %+v", dec, s.msg)
			}
		})
	}
}
