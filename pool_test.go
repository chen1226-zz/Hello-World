package poolleak

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func mustGet(t *testing.T, h http.Handler, target string) (int, http.Header, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	h.ServeHTTP(rec, req)
	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return rec.Code, rec.Result().Header, string(body)
}

// TestSequentialNoLeak mirrors `make leak`: a large, marker-bearing request
// alternates with a tiny one and the tiny response must never contain any
// fragment of the previous request.
func TestSequentialNoLeak(t *testing.T) {
	h := NewHandler()
	for i := 0; i < 1000; i++ {
		marker := fmt.Sprintf("MARKER_A_%d", i)
		_, _, bodyA := mustGet(t, h, "/pay?user=alice&amount=10000&tags="+marker+
			"&notes="+marker+"&size=4096")
		if !strings.Contains(bodyA, marker) {
			t.Fatalf("round %d: marker missing from A: %q", i, bodyA)
		}

		_, headers, bodyB := mustGet(t, h, "/pay?user=bob&amount=7")
		if strings.Contains(bodyB, "MARKER_A") || strings.Contains(bodyB, "alice") {
			t.Fatalf("round %d: body leaked previous request: %q", i, bodyB)
		}
		for _, v := range headers {
			for _, vv := range v {
				if strings.Contains(vv, "MARKER_A") || strings.Contains(vv, "alice") {
					t.Fatalf("round %d: header leaked previous request: %q", i, vv)
				}
			}
		}
	}
}

// TestErrorPathResets verifies the parse-error recycle path: a large request
// object and a half-populated response from a successful request must be
// reset before the next user acquires them.
func TestErrorPathResets(t *testing.T) {
	h := NewHandler()

	// Seed the pools with marker data and a large payload buffer.
	_, headers, body := mustGet(t, h, "/pay?user=alice&amount=9999&tags=SECRET_TAG&notes=SECRET_NOTE&size=8192")
	if !strings.Contains(body, "SECRET_TAG") {
		t.Fatalf("seed body missing marker: %q", body)
	}

	// Trigger parse errors on both pools (request parse error happens after
	// both objects were acquired; response is recycled via the error funnel).
	for i := 0; i < 50; i++ {
		code, _, errBody := mustGet(t, h, "/pay?user=bob&amount=not-a-number")
		if code != http.StatusBadRequest {
			t.Fatalf("round %d: expected 400, got %d", i, code)
		}
		if strings.Contains(errBody, "SECRET") {
			t.Fatalf("round %d: error body leaked data: %q", i, errBody)
		}
	}

	// The next successful request must observe a clean response.
	_, headers, body = mustGet(t, h, "/pay?user=carol&amount=1")
	if strings.Contains(body, "SECRET") || strings.Contains(body, "alice") || strings.Contains(body, "bob") {
		t.Fatalf("post-error body leaked data: %q", body)
	}
	for _, vals := range headers {
		for _, v := range vals {
			if strings.Contains(v, "SECRET") || strings.Contains(v, "alice") {
				t.Fatalf("post-error header leaked data: %q", v)
			}
		}
	}
}

type errBackend struct{}

func (errBackend) Process(context.Context, string, int64) error {
	return context.DeadlineExceeded
}

// TestBackendErrorPathResets verifies the backend-error recycle path.
func TestBackendErrorPathResets(t *testing.T) {
	h := &Handler{Backend: errBackend{}}
	// First acquire sequence after seeding must not surface stale data.
	_, _, _ = mustGet(t, h, "/pay?user=alice&amount=5")

	// Switch to a healthy handler and make sure objects dirtied by the failed
	// path were returned clean (they shared the same package-level pools).
	ok := NewHandler()
	_, headers, body := mustGet(t, ok, "/pay?user=carol&amount=1")
	if strings.Contains(body, "alice") {
		t.Fatalf("backend-error path left dirty response: %q", body)
	}
	for _, vals := range headers {
		for _, v := range vals {
			if strings.Contains(v, "alice") {
				t.Fatalf("backend-error path left dirty header: %q", v)
			}
		}
	}
}

// TestCanceledPathResets cancels a request while the backend is blocked. The
// in-flight response must be recycled clean so the next acquisition starts
// empty.
func TestCanceledPathResets(t *testing.T) {
	h := &Handler{Backend: NewSleepingBackend(time.Hour)}
	srv := httptest.NewServer(h)
	defer srv.Close()

	const canceled = 32
	var wg sync.WaitGroup
	for i := 0; i < canceled; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
				srv.URL+"/pay?user=victim&amount=42&tags=VICTIM_TAG", nil)
			resp, err := srv.Client().Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	// Wait for all deferred recycles to land, then use the pools through a
	// healthy handler.
	ok := &Handler{Backend: NewSleepingBackend(0)}
	_, headers, body := mustGet(t, ok, "/pay?user=clean&amount=0")
	if strings.Contains(body, "victim") || strings.Contains(body, "VICTIM") {
		t.Fatalf("canceled path left dirty response: %q", body)
	}
	for _, vals := range headers {
		for _, v := range vals {
			if strings.Contains(v, "VICTIM") || strings.Contains(v, "victim") {
				t.Fatalf("canceled path left dirty header: %q", v)
			}
		}
	}
}

// TestConcurrentReuseNoLeak hammers the pools from many goroutines with two
// distinct identities and verifies no identity ever sees the other's data.
func TestConcurrentReuseNoLeak(t *testing.T) {
	h := NewHandler()
	srv := httptest.NewServer(h)
	defer srv.Close()

	const goroutines = 32
	const perG = 100
	var wg sync.WaitGroup
	var failures int64
	var failMu sync.Mutex
	recordFailure := func(format string, args ...any) {
		failMu.Lock()
		if failures < 10 {
			t.Errorf(format, args...)
		}
		failures++
		failMu.Unlock()
	}

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			identity := "odd"
			marker := "ODD_MARKER"
			if g%2 == 0 {
				identity = "even"
				marker = "EVEN_MARKER"
			}
			for i := 0; i < perG; i++ {
				url := srv.URL + "/pay?user=" + identity + "&amount=" +
					fmt.Sprint(g*1000+i) + "&tags=" + marker + fmt.Sprint(i)
				if i%2 == 0 {
					url += "&size=2048"
				}
				resp, err := srv.Client().Get(url)
				if err != nil {
					recordFailure("request error: %v", err)
					continue
				}
				data, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				body := string(data)
				other := "even"
				otherMarker := "EVEN_MARKER"
				if g%2 == 0 {
					other = "odd"
					otherMarker = "ODD_MARKER"
				}
				if strings.Contains(body, "user="+other+"&") || strings.Contains(body, otherMarker) {
					recordFailure("goroutine %d iteration %d saw foreign data: %q", g, i, truncate(body))
				}
			}
		}(g)
	}
	wg.Wait()
	if failures > 0 {
		t.Fatalf("concurrent reuse produced %d leakage observations", failures)
	}
}

func truncate(s string) string {
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}

// TestResetContract checks the documented reset rules directly: backing
// arrays survive (so allocations stay flat) but none of their contents are
// observable after Reset.
func TestResetContract(t *testing.T) {
	t.Run("request", func(t *testing.T) {
		r := AcquireRequest()
		r.User = "alice"
		r.Amount = 123
		r.Pad = 99
		r.ctx = context.Background()
		r.Tags = append(r.Tags, "a", "b", "c")
		r.Notes = append(r.Notes, "note-secret")
		r.Scratch = append(r.Scratch, make([]byte, 4096)...)
		for i := range r.Scratch {
			r.Scratch[i] = 'Z'
		}

		tagsCap, notesCap, scratchCap := cap(r.Tags), cap(r.Notes), cap(r.Scratch)
		r.Reset()

		if r.User != "" || r.Amount != 0 || r.Pad != 0 || r.ctx != nil {
			t.Fatalf("scalar/ctx fields not reset: %+v", r)
		}
		if len(r.Tags) != 0 || len(r.Notes) != 0 || len(r.Scratch) != 0 {
			t.Fatalf("slice lengths not reset: %d %d %d", len(r.Tags), len(r.Notes), len(r.Scratch))
		}
		if cap(r.Tags) != tagsCap || cap(r.Notes) != notesCap || cap(r.Scratch) != scratchCap {
			t.Fatal("reset must retain backing arrays (allocation contract)")
		}
		if !bytes.Equal(r.Scratch[:cap(r.Scratch)], make([]byte, scratchCap)) {
			t.Fatal("scratch backing array not zeroed")
		}
		if len(r.Tags) > 0 || len(r.Notes) > 0 {
			t.Fatal("reset slices must have zero length")
		}

		// Reusing the retained arrays must not resurrect old entries.
		r.Tags = append(r.Tags, "only")
		r.Scratch = append(r.Scratch, "ab"...)
		if strings.Join(r.Tags, ",") != "only" || string(r.Scratch) != "ab" {
			t.Fatalf("stale data resurfaced after reuse: %v %q", r.Tags, r.Scratch)
		}
	})

	t.Run("response", func(t *testing.T) {
		r := AcquireResponse()
		r.Status = 200
		r.Amount = 555
		r.Note = "note-secret"
		r.Tags = append(r.Tags, "t1", "t2")
		r.Headers["X-Secret"] = "secret-value"
		r.Headers["X-Canonical"] = "canonical=alice=1"
		r.Payload = append(r.Payload, make([]byte, 4096)...)
		for i := range r.Payload {
			r.Payload[i] = 'Q'
		}

		tagsCap, payloadCap, headersLen := cap(r.Tags), cap(r.Payload), len(r.Headers)
		r.Reset()

		if r.Status != 0 || r.Amount != 0 || r.Note != "" {
			t.Fatalf("scalar fields not reset: %+v", r)
		}
		if len(r.Tags) != 0 || len(r.Payload) != 0 {
			t.Fatalf("slice lengths not reset: %d %d", len(r.Tags), len(r.Payload))
		}
		if cap(r.Tags) != tagsCap || cap(r.Payload) != payloadCap {
			t.Fatal("reset must retain backing arrays (allocation contract)")
		}
		if len(r.Headers) != 0 {
			t.Fatalf("headers not cleared: %v", r.Headers)
		}
		if headersLen == 0 {
			t.Fatal("test precondition: seeded headers expected")
		}
		if !bytes.Equal(r.Payload[:cap(r.Payload)], make([]byte, payloadCap)) {
			t.Fatal("payload backing array not zeroed")
		}

		// The reused map must behave like a fresh map.
		if v, ok := r.Headers["X-Secret"]; ok {
			t.Fatalf("cleared header still visible: %q", v)
		}
		r.Tags = append(r.Tags, "fresh")
		if strings.Join(r.Tags, ",") != "fresh" {
			t.Fatalf("stale tags resurfaced: %v", r.Tags)
		}
		r.Payload = append(r.Payload, "new"...)
		if string(r.Payload) != "new" {
			t.Fatalf("stale payload resurfaced: %q", r.Payload)
		}
	})
}

// TestLargeThenSmall checks the "larger payload makes it worse" symptom: the
// stale tail of a big backing array must never be exposed by a later small
// response.
func TestLargeThenSmall(t *testing.T) {
	h := NewHandler()
	_, _, big := mustGet(t, h, "/pay?user=alice&amount=1&notes=BIGSECRET&size=16384")
	if !strings.Contains(big, "BIGSECRET") {
		t.Fatalf("seed missing marker: %q", big)
	}
	for i := 0; i < 100; i++ {
		_, _, small := mustGet(t, h, "/pay?user=bob&amount=2")
		if strings.Contains(small, "BIGSECRET") || strings.Contains(small, "alice") {
			t.Fatalf("iteration %d: small response leaked big tail: %q", i, truncate(small))
		}
	}
}
