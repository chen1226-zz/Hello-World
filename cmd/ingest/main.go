// Command ingest is the bounded-queue ingest gateway.
//
// Configuration via environment:
//
//	UPSTREAM_URL        upstream to forward requests to (required)
//	LISTEN_ADDR         HTTP listen address (default ":8080")
//	QUEUE_CAPACITY      bounded queue size (default 512)
//	WORKERS             fixed worker pool size (default 16)
//	UPSTREAM_TIMEOUT    per-forward timeout (default 2s)
package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"ingest/internal/ingest"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func main() {
	upstreamURL := os.Getenv("UPSTREAM_URL")
	if upstreamURL == "" {
		log.Fatal("UPSTREAM_URL is required")
	}

	srv := ingest.New(
		upstreamURL,
		envInt("QUEUE_CAPACITY", ingest.DefaultQueueCapacity),
		envInt("WORKERS", ingest.DefaultWorkers),
		envDuration("UPSTREAM_TIMEOUT", ingest.DefaultUpstreamTimout),
	)
	defer srv.Close()

	httpServer := &http.Server{
		Addr:              env("LISTEN_ADDR", ":8080"),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		log.Println("shutting down")
		_ = httpServer.Close()
	}()

	log.Printf("ingest listening on %s (queue=%d workers=%d timeout=%s upstream=%s)",
		httpServer.Addr,
		envInt("QUEUE_CAPACITY", ingest.DefaultQueueCapacity),
		envInt("WORKERS", ingest.DefaultWorkers),
		envDuration("UPSTREAM_TIMEOUT", ingest.DefaultUpstreamTimout),
		upstreamURL,
	)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
