// Command hamster is the Hamster server binary.
//
// The only command so far is serve: a single-node S3 endpoint over the v0.1
// gateway. Object data lives as blobs under <data-dir>/blobs, metadata in
// BadgerDB under <data-dir>/meta (ADR-0005) — both survive a restart. Still
// a development preview: single node, no erasure coding, v0 formats may
// change between releases (ROADMAP.md).
package main

import (
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	mathrand "math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/hamster-storage/hamster/internal/blob"
	"github.com/hamster-storage/hamster/internal/gateway"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/sys"
)

// version is stamped by the release build:
//
//	go build -ldflags "-X main.version=v0.1.0" ./cmd/hamster
var version = "dev"

func main() {
	log.SetFlags(0)
	if len(os.Args) >= 2 && os.Args[1] == "version" {
		fmt.Println("hamster", version)
		return
	}
	if len(os.Args) < 2 || os.Args[1] != "serve" {
		fmt.Fprintln(os.Stderr, "usage: hamster serve [flags] | hamster version")
		os.Exit(2)
	}
	if err := serve(os.Args[2:]); err != nil {
		log.Fatalf("hamster: %v", err)
	}
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:9000", "address to serve the S3 API on")
	dataDir := fs.String("data-dir", "", "directory for object data (required)")
	region := fs.String("region", "us-east-1", "SigV4 region string")
	domain := fs.String("domain", "", "base domain for virtual-hosted bucket addressing, e.g. s3.example.com (empty: path-style only)")
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
		Store: store,
		Loop:  loop,
		Clock: sys.Clock{},
		Rand:  rng,
		Blobs: blob.NewStore(disk),
	})

	srv := &http.Server{Addr: *listen, Handler: g}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe() }()

	log.Printf("hamster serve: %s — S3 API on http://%s (region %s)", version, *listen, *region)
	log.Printf("hamster serve: data in %s (%d metadata rows restored)", *dataDir, restored)
	log.Printf("hamster serve: DEV PREVIEW — single node, v0 formats may change between releases")

	select {
	case err := <-done:
		return err
	case <-stop:
	}
	// Shutdown order per the gateway contract: HTTP first, loop second —
	// and the metadata db last, once nothing can post a transaction.
	if err := srv.Close(); err != nil {
		return err
	}
	loop.Stop()
	return mdb.Close()
}
