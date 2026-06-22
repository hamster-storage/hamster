package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/hamster-storage/hamster/internal/metrics"
)

// startAdmin starts the admin HTTP server (ADR-0035): plain HTTP on the admin
// port, serving the Prometheus text exposition at /metrics from reg. The admin
// port is the operational surface — the web console joins it here in a later
// release (ADR-0020). It is deliberately separate from the S3 data port. Returns
// the server so the caller can shut it down on exit.
func startAdmin(addr string, reg *metrics.Registry) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		// The version=0.0.4 content type is the Prometheus text exposition format
		// scrapers expect.
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := reg.WritePrometheus(w); err != nil {
			log.Printf("admin: writing metrics: %v", err)
		}
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("admin: server on %s stopped: %v", addr, err)
		}
	}()
	return srv
}

// shutdownAdmin gracefully stops the admin server, if one is running.
func shutdownAdmin(srv *http.Server) {
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
