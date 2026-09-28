package poolleak

import (
	"net/http/httptest"
	"testing"
)

// BenchmarkHandleAlternating drives the same large/small request pattern as
// `make leak` directly through Handler.Handle, reporting allocations per
// request: the pooled design must stay allocation-flat in steady state
// without resorting to per-request deep copies.
func BenchmarkHandleAlternating(b *testing.B) {
	h := NewHandler()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var target string
		if i%2 == 0 {
			target = "/pay?user=alice&amount=10000&tags=audit,vip&notes=ok&size=4096"
		} else {
			target = "/pay?user=bob&amount=7"
		}
		req := httptest.NewRequest("GET", target, nil)
		resp, err := h.Handle(req)
		if err != nil {
			b.Fatal(err)
		}
		if len(resp.Payload) == 0 {
			b.Fatal("empty payload")
		}
		h.Release(resp)
	}
}

func BenchmarkServeHTTPAlternating(b *testing.B) {
	h := NewHandler()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		target := "/pay?user=alice&amount=10000&tags=audit,vip&notes=ok&size=4096"
		if i%2 == 1 {
			target = "/pay?user=bob&amount=7"
		}
		req := httptest.NewRequest("GET", target, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
	}
}

// BenchmarkPoolReset shows that acquiring, filling, resetting and recycling
// pooled objects allocates nothing in steady state: the fix is reset-based,
// not a per-request deep copy.
func BenchmarkPoolReset(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := AcquireRequest()
		req.User = "alice"
		req.Amount = 100
		req.Tags = append(req.Tags, "a", "b")
		req.Scratch = append(req.Scratch, make([]byte, 4096)...)
		recycleRequest(req)

		resp := AcquireResponse()
		resp.Status = 200
		resp.Note = "ok"
		resp.Tags = append(resp.Tags, "t")
		resp.Headers["X"] = "y"
		resp.Payload = append(resp.Payload, make([]byte, 4096)...)
		NewHandler().recycleResponse(resp)
	}
}
