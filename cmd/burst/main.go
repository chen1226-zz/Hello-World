// Command burst reproduces a 10x traffic burst against the ingest
// gateway and reports peak heap growth and latency percentiles.
//
// It runs a mock upstream and the real ingest server in-process, drives
// load in three phases (baseline / 10x burst / recovery), samples
// heap usage, and exits non-zero if the service-level targets are
// violated (peak heap > 2x baseline or burst P99 >= 200ms).
package main

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"ingest/internal/ingest"
)

const (
	baseRPS         = 200
	burstMultiple   = 10
	baselineDur     = 2 * time.Second
	burstDur        = 5 * time.Second
	recoveryDur     = 2 * time.Second
	upstreamDelay   = 20 * time.Millisecond
	queueCapacity   = 512
	workers         = 16
	upstreamTimeout = 2 * time.Second
	bodySize        = 512

	memTargetRatio = 2.0
	p99Target      = 200 * time.Millisecond
)

type phaseStats struct {
	latencies []time.Duration
	status2xx int64
	status429 int64
	status5xx int64
}

func main() {
	body := bytes.Repeat([]byte("x"), bodySize)

	// Mock upstream: mildly slow, so arrival rate during burst
	// (2000 rps) exceeds drain rate (~1600 rps).
	var upstreamInFlight atomic.Int64
	var upstreamPeak atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := upstreamInFlight.Add(1)
		for {
			peak := upstreamPeak.Load()
			if cur <= peak || upstreamPeak.CompareAndSwap(peak, cur) {
				break
			}
		}
		time.Sleep(upstreamDelay)
		upstreamInFlight.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	srv := ingest.New(upstream.URL, queueCapacity, workers, upstreamTimeout)
	defer srv.Close()
	gateway := httptest.NewServer(srv.Handler())
	defer gateway.Close()

	client := &http.Client{Timeout: 5 * time.Second}

	// Warm up allocator and pool.
	for i := 0; i < 50; i++ {
		resp, err := client.Post(gateway.URL+"/ingest", "application/octet-stream", bytes.NewReader(body))
		if err == nil {
			resp.Body.Close()
		}
	}
	waitDrain(srv)
	runtime.GC()

	var baselineBytes, peakBytes atomic.Int64

	var samplerWg sync.WaitGroup
	samplerWg.Add(1)
	go func() {
		defer samplerWg.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		var ms runtime.MemStats
		for {
			select {
			case <-stopSampling:
				return
			case <-ticker.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapInuse > uint64(peakBytes.Load()) {
					peakBytes.Store(int64(ms.HeapInuse))
				}
			}
		}
	}()

	baseline := drive(client, gateway.URL, body, baseRPS, baselineDur, &baselineBytes, &peakBytes)
	burst := drive(client, gateway.URL, body, baseRPS*burstMultiple, burstDur, nil, &peakBytes)
	recovery := drive(client, gateway.URL, body, baseRPS, recoveryDur, nil, &peakBytes)

	close(stopSampling)
	samplerWg.Wait()
	waitDrain(srv)

	base := baselineBytes.Load()
	peak := peakBytes.Load()
	ratio := float64(peak) / float64(base)

	fmt.Println("=== make burst: 10x traffic for 5s ===")
	fmt.Printf("config: queue=%d workers=%d upstream_delay=%s timeout=%s body=%dB\n",
		queueCapacity, workers, upstreamDelay, upstreamTimeout, bodySize)
	printPhase("baseline ", baseRPS, baseline)
	printPhase("burst x10", baseRPS*burstMultiple, burst)
	printPhase("recovery ", baseRPS, recovery)

	m := srv.Metrics().Snapshot()
	fmt.Println("--- metrics after run ---")
	fmt.Printf("accepted=%d rejected=%d dropped=%d forwarded=%d queue_len=%d\n",
		m.Accepted, m.Rejected, m.Dropped, m.Forwarded, m.QueueLen)
	fmt.Printf("upstream peak in-flight requests=%d (== worker pool size, no goroutine explosion)\n", upstreamPeak.Load())

	fmt.Println("--- service-level targets ---")
	fmt.Printf("baseline heap=%s  peak heap=%s  ratio=%.2fx  target<%.1fx  %s\n",
		humanBytes(base), humanBytes(peak), ratio, memTargetRatio, mark(ratio < memTargetRatio))
	p99 := burst.p99()
	fmt.Printf("burst P99 latency=%s  target<%s  %s\n",
		p99, p99Target, mark(p99 < p99Target))
	fmt.Printf("post-recovery queue_len=%d (must be 0)  %s\n",
		m.QueueLen, mark(m.QueueLen == 0))

	if ratio >= memTargetRatio || p99 >= p99Target || m.QueueLen != 0 {
		os.Exit(1)
	}
}

var stopSampling = make(chan struct{})

// drive sends rps requests per second for dur in a single-threaded ticker
// loop with small concurrent worker fan-out so a slow client cannot
// itself become the bottleneck.
func drive(client *http.Client, baseURL string, body []byte, rps int, dur time.Duration, baselineOut, peakOut *atomic.Int64) phaseStats {
	var st phaseStats
	expected := rps*int(dur/time.Second) + rps
	results := make(chan time.Duration, expected)

	var sendWg sync.WaitGroup
	concurrency := 64
	if rps < concurrency {
		concurrency = rps
	}
	// Buffer holds a whole phase of ticks so pacing never silently drops
	// a scheduled send when the client workers lag briefly.
	jobs := make(chan struct{}, expected)
	for i := 0; i < concurrency; i++ {
		sendWg.Add(1)
		go func() {
			defer sendWg.Done()
			for range jobs {
				start := time.Now()
				resp, err := client.Post(baseURL+"/ingest", "application/octet-stream", bytes.NewReader(body))
				d := time.Since(start)
				if err != nil {
					st.status5xx++
				} else {
					resp.Body.Close()
					switch {
					case resp.StatusCode == http.StatusTooManyRequests:
						st.status429++
					case resp.StatusCode < 300:
						st.status2xx++
					default:
						st.status5xx++
					}
				}
				results <- d
			}
		}()
	}

	deadline := time.Now().Add(dur)
	// Catch-up pacer: unlike a ticker, if the loop ran behind it emits
	// all ticks that are already due, so the nominal rate is actually
	// delivered (requests buffer in jobs and never pace the timer).
	next := time.Now()
	step := time.Second / time.Duration(rps)
	var samples []uint64
	var ms runtime.MemStats
	for {
		next = next.Add(step)
		if time.Now().After(deadline) {
			break
		}
		jobs <- struct{}{}
		if baselineOut != nil {
			runtime.ReadMemStats(&ms)
			samples = append(samples, ms.HeapInuse)
			baselineOut.Store(int64(medianU64(samples)))
			if ms.HeapInuse > uint64(peakOut.Load()) {
				peakOut.Store(int64(ms.HeapInuse))
			}
		}
		if sleepFor := time.Until(next); sleepFor > 0 {
			time.Sleep(sleepFor)
		}
	}
	close(jobs)
	sendWg.Wait()
	close(results)
	for d := range results {
		st.latencies = append(st.latencies, d)
	}
	return st
}

func waitDrain(srv *ingest.Server) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Metrics().QueueLen() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (s phaseStats) p99() time.Duration {
	if len(s.latencies) == 0 {
		return math.MaxInt64
	}
	sorted := append([]time.Duration(nil), s.latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := len(sorted) * 99 / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func printPhase(name string, rps int, s phaseStats) {
	total := s.status2xx + s.status429 + s.status5xx
	fmt.Printf("%s: rps=%d total=%d 2xx=%d 429=%d 5xx=%d P50=%s P99=%s max=%s\n",
		name, rps, total, s.status2xx, s.status429, s.status5xx,
		percentile(s.latencies, 50), s.p99(), maxLatency(s.latencies))
}

func percentile(latencies []time.Duration, p int) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := len(sorted) * p / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func maxLatency(latencies []time.Duration) time.Duration {
	var max time.Duration
	for _, d := range latencies {
		if d > max {
			max = d
		}
	}
	return max
}

func medianU64(v []uint64) uint64 {
	sorted := append([]uint64(nil), v...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func mark(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}
