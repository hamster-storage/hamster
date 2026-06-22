// Command hamster is the Hamster server binary.
//
// serve runs a single-node S3 endpoint over the v0.1 gateway: object data
// as blobs under <data-dir>/blobs, metadata in BadgerDB under
// <data-dir>/meta (ADR-0005) — both survive a restart.
//
// cluster manages the v0.2 metadata cluster preview (internal/cluster):
// init mints the cluster CA and the first node, token and join grow it
// (ADR-0022), run starts a node, status shows membership. The S3 gateway
// stays on the single-node path until the erasure-coded data path arrives
// (v0.3).
//
// Still a development preview: v0 formats may change between releases
// (ROADMAP.md).
package main

import (
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"log/slog"
	mathrand "math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/hamster-storage/hamster/internal/blob"
	"github.com/hamster-storage/hamster/internal/gateway"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/sys"
)

// version is stamped by the release build:
//
//	go build -ldflags "-X main.version=v0.1.0" ./cmd/hamster
var version = "dev"

// protocolGenerationStr is the declared protocol generation (ADR-0034): the
// monotonic integer this binary owns, advanced only by a coordinated format
// change (not every release). The cluster's effective generation is the minimum
// across live members, etcd-style, and a gate (when one first lands) compares
// against it. Stamped like version — `-ldflags "-X main.protocolGenerationStr=2"`
// — so the upgrade test can build two generations from one source; defaults to
// the baseline generation 1.
var protocolGenerationStr = "1"

// protocolGeneration parses protocolGenerationStr, falling back to the baseline
// on a malformed stamp (a build-time string, so this is defensive only).
func protocolGeneration() uint32 {
	g, err := strconv.ParseUint(protocolGenerationStr, 10, 32)
	if err != nil {
		return 1
	}
	return uint32(g)
}

// fullVersion is what banners and `hamster version` print: the stamped
// release version as-is, or — for a plain `go build` — the commit Go
// embedded in the binary (git tags are not part of Go's VCS stamping, so
// a dev build can name its commit but not the latest tag).
func fullVersion() string {
	if version != "dev" {
		return version
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return version
	}
	var revision, modified string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	if revision == "" {
		return version
	}
	v := "dev (" + revision[:min(7, len(revision))]
	if modified == "true" {
		v += ", modified"
	}
	return v + ")"
}

// usage is the top-level help: what hamster is, then its commands grouped the
// way an operator reaches for them.
const usage = `hamster is a self-hosted, S3-compatible object store in a single binary.

Learn more: https://github.com/hamster-storage/hamster

Usage:
  hamster <command> [flags]

Basic commands:
  serve              run a single-node S3 endpoint (dev preview; one disk)
  version            print the version

Cluster commands:
  cluster init       found a new cluster (mints the cluster CA)
  cluster token      mint a single-use join token (on the init node)
  cluster join       join an existing cluster with a token
  cluster run        run this node, serving the S3 API (-no-s3 for a headless storage node)
  cluster status     show cluster membership
  cluster can-stop   check whether a node is safe to stop for upgrade (ADR-0034)
  cluster recover    rebuild a cluster from a survivor after quorum loss

Use "hamster <command> -h" for a command's flags.
`

func main() {
	setupLogging()
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "version":
		fmt.Println("hamster", fullVersion())
	case "serve":
		err = serve(os.Args[2:])
	case "cluster":
		err = clusterCmd(os.Args[2:])
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		log.Fatalf("hamster: %v", err)
	}
}

// setupLogging configures the process logger. The default is plain, timestamped
// lines for a human at a terminal; HAMSTER_LOG_FORMAT=json switches to one JSON
// record per line for log shippers in production. Both carry a timestamp — the
// previous default carried none.
func setupLogging() {
	if strings.EqualFold(os.Getenv("HAMSTER_LOG_FORMAT"), "json") {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
		// Route the standard log package (log.Printf throughout the server)
		// through slog so every line becomes one JSON record, without
		// rewriting the call sites.
		log.SetFlags(0)
		log.SetOutput(slogLogWriter{})
		return
	}
	log.SetFlags(log.LstdFlags)
}

// slogLogWriter forwards standard-library log output to slog as Info records,
// so existing log.Printf calls emit JSON in production mode.
type slogLogWriter struct{}

func (slogLogWriter) Write(p []byte) (int, error) {
	slog.Default().Info(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// logRequests wraps the gateway with one access-log line per request:
// status, method, path, response size, duration, and the request ID the
// gateway minted (the same ID an S3 error body carries, so a client error
// report lines up with the server log).
func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		log.Printf("%d %s %s (%dB in %s) request-id %s",
			rec.status, r.Method, r.URL.RequestURI(), rec.bytes,
			time.Since(start).Round(time.Microsecond), rec.Header().Get("x-amz-request-id"))
	})
}

// statusRecorder captures what the handler wrote, for the access log.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	n, err := r.ResponseWriter.Write(p)
	r.bytes += int64(n)
	return n, err
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:9000", "address to serve the S3 API on")
	dataDir := fs.String("data-dir", "", "directory for object data (required)")
	region := fs.String("region", "us-east-1", "SigV4 region string")
	domain := fs.String("domain", "", "base domain for virtual-hosted bucket addressing, e.g. s3.example.com (empty: path-style only)")
	admin := fs.String("admin", "", "serve the admin endpoints on this address (host:port): Prometheus metrics at /metrics (ADR-0035); empty disables")
	fs.Parse(args)

	if *dataDir == "" {
		return fmt.Errorf("-data-dir is required")
	}
	accessKey := os.Getenv("HAMSTER_ACCESS_KEY_ID")
	secretKey := os.Getenv("HAMSTER_SECRET_ACCESS_KEY")
	if accessKey == "" || secretKey == "" {
		return fmt.Errorf("set HAMSTER_ACCESS_KEY_ID and HAMSTER_SECRET_ACCESS_KEY")
	}

	disk, err := sys.NewDisk(filepath.Join(*dataDir, "blobs"))
	if err != nil {
		return err
	}
	mdb, err := sys.OpenMetaDB(filepath.Join(*dataDir, "meta"))
	if err != nil {
		return err
	}
	store := meta.NewStore()
	restored := 0
	if err := mdb.Load(func(k string, v []byte) error {
		restored++
		return store.Restore(k, v)
	}); err != nil {
		return err
	}
	store.SetPersister(mdb)
	loop := sys.NewLoop()

	// The composition root is where ambient entropy is allowed: it seeds
	// the PRNG explicitly and hands it in, so everything below stays
	// deterministic under the simulator.
	var seed [16]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return err
	}
	rng := mathrand.New(mathrand.NewPCG(
		binary.LittleEndian.Uint64(seed[0:8]), binary.LittleEndian.Uint64(seed[8:16])))

	g := gateway.New(gateway.Config{
		Region: *region,
		Domain: *domain,
		Lookup: func(akid string) (string, bool) {
			if akid == accessKey {
				return secretKey, true
			}
			return "", false
		},
		Clock: sys.Clock{},
		Meta:  gateway.NewLoopMetadata(store, loop, sys.Clock{}, rng),
		Blobs: blob.NewStore(disk),
	})

	srv := &http.Server{Addr: *listen, Handler: logRequests(g)}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe() }()

	var adminSrv *http.Server
	if *admin != "" {
		adminSrv = startAdmin(*admin, newProcessRegistry(fullVersion(), protocolGeneration(), time.Now()))
		log.Printf("hamster serve: admin endpoints on http://%s (metrics at /metrics)", *admin)
	}

	log.Printf("hamster serve: %s — S3 API on http://%s (region %s)", fullVersion(), *listen, *region)
	log.Printf("hamster serve: data in %s (%d metadata rows restored)", *dataDir, restored)
	log.Printf("hamster serve: DEV PREVIEW — single node, v0 formats may change between releases")

	select {
	case err := <-done:
		shutdownAdmin(adminSrv)
		return err
	case <-stop:
	}
	log.Printf("hamster serve: shutting down")
	// Shutdown order per the gateway contract: HTTP first, loop second —
	// and the metadata db last, once nothing can post a transaction.
	shutdownAdmin(adminSrv)
	if err := srv.Close(); err != nil {
		return err
	}
	loop.Stop()
	return mdb.Close()
}
