package wire

import (
	"math/rand"
	"testing"
)

// FuzzDecode is the go-fuzz entry point: any byte sequence must either
// decode or produce an error, never panic.
func FuzzDecode(f *testing.F) {
	f.Add(V2{ID: 1, Name: "seed", Priority: 3}.Encode())
	f.Add([]byte{Magic0, Magic1, 0x01})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeV1(data)
		_, _ = DecodeV2(data)
	})
}

// TestRandomBytesNoPanic hammers both decoders with 100k random inputs:
// pure garbage plus random mutations of valid frames.
func TestRandomBytesNoPanic(t *testing.T) {
	rng := rand.New(rand.NewSource(20260928))
	valid := [][]byte{
		V1{ID: 1, Name: "hello"}.Encode(),
		V2{ID: 9, Name: "world", Priority: 5}.Encode(),
	}
	for i := 0; i < 100000; i++ {
		var data []byte
		if i%2 == 0 {
			data = make([]byte, rng.Intn(64))
			rng.Read(data)
		} else {
			base := valid[rng.Intn(len(valid))]
			data = make([]byte, len(base))
			copy(data, base)
			for m := rng.Intn(4) + 1; m > 0 && len(data) > 0; m-- {
				data[rng.Intn(len(data))] = byte(rng.Intn(256))
			}
			if cut := rng.Intn(len(data) + 1); cut < len(data) {
				data = data[:cut]
			}
		}
		_, _ = DecodeV1(data)
		_, _ = DecodeV2(data)
	}
}
