package wire

import "testing"

// TestInteropMatrix exercises both encode/decode directions across
// protocol versions and asserts field-level equality.
func TestInteropMatrix(t *testing.T) {
	v1Msgs := []V1{
		{ID: 1, Name: "hello"},
		{ID: 0, Name: ""},
		{ID: 300, Name: "multi-byte varint id"},
		{ID: 1<<63 + 7, Name: "large id"},
	}
	v2Msgs := []V2{
		{ID: 1, Name: "hello", Priority: 9},
		{ID: 0, Name: "", Priority: 0},
		{ID: 42, Name: "default", Priority: DefaultPriority},
		{ID: 1<<63 + 7, Name: "large id", Priority: 255},
	}

	t.Run("v1->v1", func(t *testing.T) {
		for _, want := range v1Msgs {
			got, err := DecodeV1(want.Encode())
			if err != nil {
				t.Fatalf("DecodeV1: %v", err)
			}
			if got.ID != want.ID || got.Name != want.Name {
				t.Errorf("got %+v, want %+v", got, want)
			}
			if len(got.Unknown) != 0 {
				t.Errorf("unexpected unknown fields: %+v", got.Unknown)
			}
		}
	})

	t.Run("v2->v2", func(t *testing.T) {
		for _, want := range v2Msgs {
			got, err := DecodeV2(want.Encode())
			if err != nil {
				t.Fatalf("DecodeV2: %v", err)
			}
			if got.ID != want.ID || got.Name != want.Name || got.Priority != want.Priority {
				t.Errorf("got %+v, want %+v", got, want)
			}
			if len(got.Unknown) != 0 {
				t.Errorf("unexpected unknown fields: %+v", got.Unknown)
			}
		}
	})

	// New server -> old client: the v1 decoder must recover every field
	// it knows and skip (but preserve) the optional v2 field.
	t.Run("v2->v1", func(t *testing.T) {
		for _, msg := range v2Msgs {
			got, err := DecodeV1(msg.Encode())
			if err != nil {
				t.Fatalf("DecodeV1(v2 frame): %v", err)
			}
			if got.ID != msg.ID || got.Name != msg.Name {
				t.Errorf("known fields misaligned: got %+v, want id=%d name=%q", got, msg.ID, msg.Name)
			}
			if len(got.Unknown) != 1 || got.Unknown[0].ID != fieldPriority {
				t.Errorf("optional v2 field not preserved as unknown: %+v", got.Unknown)
			}
		}
	})

	// Old client -> new server: the v2 decoder must accept frames
	// lacking the optional field and fill in the default.
	t.Run("v1->v2", func(t *testing.T) {
		for _, msg := range v1Msgs {
			got, err := DecodeV2(msg.Encode())
			if err != nil {
				t.Fatalf("DecodeV2(v1 frame): %v", err)
			}
			if got.ID != msg.ID || got.Name != msg.Name {
				t.Errorf("got %+v, want id=%d name=%q", got, msg.ID, msg.Name)
			}
			if got.Priority != DefaultPriority {
				t.Errorf("Priority = %d, want default %d", got.Priority, DefaultPriority)
			}
			if len(got.Unknown) != 0 {
				t.Errorf("unexpected unknown fields: %+v", got.Unknown)
			}
		}
	})
}
