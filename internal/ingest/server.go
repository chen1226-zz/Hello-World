// Package ingest implements a bounded-queue ingest gateway.
//
// Requests are accepted on POST /ingest, placed on a bounded in-memory
// queue, and forwarded to an upstream by a fixed-size worker pool with a
// per-request timeout. When the queue is full the server fails fast with
// HTTP 429 instead of buffering unboundedly.
package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// Default tuning knobs. QueueCapacity bounds the number of in-flight
// requests and therefore bounds memory growth under burst.
const (
	DefaultQueueCapacity  = 512
	DefaultWorkers        = 16
	DefaultUpstreamTimout = 2 * time.Second
	MaxBodyBytes          = 1 << 20 // 1 MiB
)

// Metrics exposes observable state. All fields are safe for concurrent use.
type Metrics struct {
	queueLen  atomic.Int64 // current number of jobs waiting in the queue
	accepted  atomic.Int64 // requests enqueued successfully
	rejected  atomic.Int64 // requests refused fast because the queue was full
	dropped   atomic.Int64 // accepted jobs that failed to be forwarded (timeout/upstream error)
	forwarded atomic.Int64 // jobs successfully delivered upstream
}

func (m *Metrics) QueueLen() int64  { return m.queueLen.Load() }
func (m *Metrics) Accepted() int64  { return m.accepted.Load() }
func (m *Metrics) Rejected() int64  { return m.rejected.Load() }
func (m *Metrics) Dropped() int64   { return m.dropped.Load() }
func (m *Metrics) Forwarded() int64 { return m.forwarded.Load() }

// Snapshot is the JSON view of Metrics served by GET /metrics.
type Snapshot struct {
	QueueLen  int64 `json:"queue_length"`
	Accepted  int64 `json:"accepted_total"`
	Rejected  int64 `json:"rejected_total"`
	Dropped   int64 `json:"dropped_total"`
	Forwarded int64 `json:"forwarded_total"`
}

func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		QueueLen:  m.queueLen.Load(),
		Accepted:  m.accepted.Load(),
		Rejected:  m.rejected.Load(),
		Dropped:   m.dropped.Load(),
		Forwarded: m.forwarded.Load(),
	}
}

type job struct {
	body []byte
}

// Server is the ingest gateway. Create it with New and shut it down with
// Close.
type Server struct {
	upstreamURL string
	client      *http.Client
	timeout     time.Duration
	queue       chan job
	metrics     *Metrics
	done        chan struct{}
	workersDone chan struct{}
}

// New creates a Server with a bounded queue of queueCapacity, a fixed pool
// of workers, and a per-forward timeout. The returned server must be
// closed with Close to stop its workers.
func New(upstreamURL string, queueCapacity, workers int, timeout time.Duration) *Server {
	if queueCapacity <= 0 {
		queueCapacity = DefaultQueueCapacity
	}
	if workers <= 0 {
		workers = DefaultWorkers
	}
	if timeout <= 0 {
		timeout = DefaultUpstreamTimout
	}
	s := &Server{
		upstreamURL: upstreamURL,
		client:      &http.Client{},
		timeout:     timeout,
		queue:       make(chan job, queueCapacity),
		metrics:     &Metrics{},
		done:        make(chan struct{}),
		workersDone: make(chan struct{}),
	}
	workersDone := make([]chan struct{}, workers)
	for i := 0; i < workers; i++ {
		workersDone[i] = make(chan struct{})
		go s.worker(workersDone[i])
	}
	go func() {
		for _, ch := range workersDone {
			<-ch
		}
		close(s.workersDone)
	}()
	return s
}

// Metrics returns the server's metrics.
func (s *Server) Metrics() *Metrics { return s.metrics }

// Handler returns the HTTP handler: POST /ingest and GET /metrics.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /ingest", s.handleIngest)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	return mux
}

// Close stops accepting work and waits for workers to drain the queue.
func (s *Server) Close() {
	close(s.done)
	<-s.workersDone
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}
	// Non-blocking enqueue: if the queue is full we fail fast with 429
	// rather than growing memory unboundedly.
	select {
	case s.queue <- job{body: body}:
		s.metrics.queueLen.Add(1)
		s.metrics.accepted.Add(1)
		w.WriteHeader(http.StatusAccepted)
	case <-s.done:
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
	default:
		s.metrics.rejected.Add(1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "overloaded", http.StatusTooManyRequests)
	}
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.metrics.Snapshot())
}

func (s *Server) worker(done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case <-s.done:
			// Drain remaining jobs before exiting.
			for {
				select {
				case j := <-s.queue:
					s.metrics.queueLen.Add(-1)
					s.forward(j)
				default:
					return
				}
			}
		case j := <-s.queue:
			s.metrics.queueLen.Add(-1)
			s.forward(j)
		}
	}
}

func (s *Server) forward(j job) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.upstreamURL, bytes.NewReader(j.body))
	if err != nil {
		s.metrics.dropped.Add(1)
		return
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := s.client.Do(req)
	if err != nil {
		// Timeout or upstream failure: the job is dropped and counted.
		s.metrics.dropped.Add(1)
		return
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
	resp.Body.Close()
	if resp.StatusCode >= 500 {
		s.metrics.dropped.Add(1)
		return
	}
	s.metrics.forwarded.Add(1)
}
