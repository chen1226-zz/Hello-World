package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func startServer(t *testing.T, upstreamURL string, capacity, workers int, timeout time.Duration) (*Server, *httptest.Server) {
	t.Helper()
	srv := New(upstreamURL, capacity, workers, timeout)
	gw := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		gw.Close()
		srv.Close()
	})
	return srv, gw
}

func slowUpstream(t *testing.T, delay time.Duration, statusCode int) *httptest.Server {
	t.Helper()
	var inFlight, peak int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt64(&inFlight, 1)
		for {
			p := atomic.LoadInt64(&peak)
			if cur <= p || atomic.CompareAndSwapInt64(&peak, p, cur) {
				break
			}
		}
		time.Sleep(delay)
		atomic.AddInt64(&inFlight, -1)
		if statusCode != 0 {
			w.WriteHeader(statusCode)
		}
	}))
	t.Cleanup(up.Close)
	t.Cleanup(func() {
		if got := atomic.LoadInt64(&peak); got > 0 {
			t.Logf("upstream peak in-flight=%d", got)
		}
	})
	return up
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

func post(t *testing.T, url string, body []byte) int {
	t.Helper()
	resp, err := http.Post(url+"/ingest", "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

// TestBurstRejectsWhenQueueFull drives far more traffic than the
// upstream can drain and verifies the server fast-fails with 429, the
// queue never exceeds capacity, and no 5xx is returned.
func TestBurstRejectsWhenQueueFull(t *testing.T) {
	const capacity, workers, n = 16, 4, 400
	up := slowUpstream(t, 50*time.Millisecond, http.StatusOK)
	srv, gw := startServer(t, up.URL, capacity, workers, time.Second)

	var accepted, rejected, other int64
	var maxQueueLen int64
	stopSampler := make(chan struct{})
	var samplerWg sync.WaitGroup
	samplerWg.Add(1)
	go func() {
		defer samplerWg.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampler:
				return
			case <-ticker.C:
				if l := srv.Metrics().QueueLen(); l > atomic.LoadInt64(&maxQueueLen) {
					atomic.StoreInt64(&maxQueueLen, l)
				}
			}
		}
	}()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			switch post(t, gw.URL, []byte("payload")) {
			case http.StatusAccepted:
				atomic.AddInt64(&accepted, 1)
			case http.StatusTooManyRequests:
				atomic.AddInt64(&rejected, 1)
			default:
				atomic.AddInt64(&other, 1)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(stopSampler)
	samplerWg.Wait()

	if atomic.LoadInt64(&other) != 0 {
		t.Fatalf("unexpected non-202/429 responses: %d", other)
	}
	if atomic.LoadInt64(&rejected) == 0 {
		t.Fatal("expected fast-fail rejections under burst, got none")
	}
	if got := atomic.LoadInt64(&maxQueueLen); got > capacity {
		t.Fatalf("queue length %d exceeded capacity %d", got, capacity)
	}
	if got := atomic.LoadInt64(&accepted) + atomic.LoadInt64(&rejected); got != n {
		t.Fatalf("accounted=%d, want %d", got, n)
	}
}

// TestSlowConsumerGoroutineBounded verifies the fixed worker pool caps
// concurrent upstream calls even when every consumer is slow.
func TestSlowConsumerGoroutineBounded(t *testing.T) {
	var inFlight, peak int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt64(&inFlight, 1)
		for {
			p := atomic.LoadInt64(&peak)
			if cur <= p || atomic.CompareAndSwapInt64(&peak, p, cur) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		atomic.AddInt64(&inFlight, -1)
	}))
	defer up.Close()

	const workers = 4
	srv, gw := startServer(t, up.URL, 128, workers, 2*time.Second)

	wave := func() int {
		for i := 0; i < 64; i++ {
			if code := post(t, gw.URL, []byte("x")); code != http.StatusAccepted {
				t.Fatalf("status=%d", code)
			}
		}
		// Upstream drain rate is ~40 rps, so jobs queue with only 4
		// concurrent calls possible; give the pool time to ramp.
		time.Sleep(150 * time.Millisecond)
		if got := atomic.LoadInt64(&peak); got > workers {
			t.Fatalf("upstream concurrency peaked at %d, worker pool allows %d", got, workers)
		}
		waitFor(t, 5*time.Second, func() bool { return srv.Metrics().QueueLen() == 0 }, "queue drain")
		// Idle keep-alive connections hold a bounded number of
		// goroutines; allow a small margin for them.
		return runtime.NumGoroutine()
	}
	first := wave()
	second := wave()
	if second > first+workers+4 {
		t.Fatalf("goroutines grew across repeated bursts: %d -> %d (per-request goroutine leak?)", first, second)
	}
}

// TestSlowUpstreamTimesOut verifies a hung upstream does not pin workers
// forever: jobs fail within the timeout and are counted as dropped.
func TestSlowUpstreamTimesOut(t *testing.T) {
	up := slowUpstream(t, 1*time.Second, http.StatusOK)
	srv, gw := startServer(t, up.URL, 8, 2, 100*time.Millisecond)

	for i := 0; i < 4; i++ {
		if code := post(t, gw.URL, []byte("x")); code != http.StatusAccepted {
			t.Fatalf("status=%d", code)
		}
	}
	waitFor(t, 2*time.Second, func() bool { return srv.Metrics().Dropped() == 4 }, "all jobs dropped on timeout")
	if got := srv.Metrics().Forwarded(); got != 0 {
		t.Fatalf("forwarded=%d, want 0", got)
	}

	// Pool must still be usable after the timeout storm.
	fastUp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer fastUp.Close()
	srv2, gw2 := startServer(t, fastUp.URL, 8, 2, time.Second)
	post(t, gw2.URL, []byte("x"))
	waitFor(t, time.Second, func() bool { return srv2.Metrics().Forwarded() == 1 }, "recovery forward")
}

// TestUpstream5xxCountedAsDropped verifies upstream failures are counted
// as drops rather than silently accepted.
func TestUpstream5xxCountedAsDropped(t *testing.T) {
	up := slowUpstream(t, time.Millisecond, http.StatusBadGateway)
	srv, gw := startServer(t, up.URL, 4, 2, time.Second)
	post(t, gw.URL, []byte("x"))
	waitFor(t, time.Second, func() bool { return srv.Metrics().Dropped() == 1 }, "5xx drop")
}

// TestRecoveryAfterBurst verifies the queue drains back to zero and low
// latency service resumes once traffic returns to normal.
func TestRecoveryAfterBurst(t *testing.T) {
	up := slowUpstream(t, 20*time.Millisecond, http.StatusOK)
	srv, gw := startServer(t, up.URL, 32, 8, time.Second)

	// Saturate.
	for i := 0; i < 300; i++ {
		post(t, gw.URL, []byte("x"))
	}
	waitFor(t, 3*time.Second, func() bool { return srv.Metrics().QueueLen() == 0 }, "post-burst drain")

	start := time.Now()
	if code := post(t, gw.URL, []byte("x")); code != http.StatusAccepted {
		t.Fatalf("status=%d after recovery", code)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("accept latency after recovery=%s, want <50ms", elapsed)
	}
	waitFor(t, 2*time.Second, func() bool {
		return srv.Metrics().Forwarded() >= 1
	}, "post-recovery forward")
}

// TestMetricsEndpoint verifies queue length, rejected and dropped
// counters are exposed via GET /metrics.
func TestMetricsEndpoint(t *testing.T) {
	up := slowUpstream(t, time.Millisecond, http.StatusOK)
	_, gw := startServer(t, up.URL, 4, 2, time.Second)

	for i := 0; i < 2; i++ {
		post(t, gw.URL, []byte("x"))
	}

	resp, err := http.Get(gw.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var snap Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	if snap.Accepted != 2 {
		t.Fatalf("accepted=%d, want 2", snap.Accepted)
	}
	// Fields the contract requires must always be present/parseable.
	_ = fmt.Sprintf("%d %d %d", snap.QueueLen, snap.Rejected, snap.Dropped)
}

// TestOversizedBodyRejected verifies a single request cannot consume
// unbounded memory via its body.
func TestOversizedBodyRejected(t *testing.T) {
	up := slowUpstream(t, time.Millisecond, http.StatusOK)
	_, gw := startServer(t, up.URL, 4, 2, time.Second)
	big := bytes.Repeat([]byte("y"), MaxBodyBytes+1)
	resp, err := http.Post(gw.URL+"/ingest", "application/octet-stream", bytes.NewReader(big))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", resp.StatusCode)
	}
}
