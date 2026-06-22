//go:build e2e

package e2e

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestClusterMetadataPersistsToBadgerDB is a normal-usage regression guard
// for ADR-0005: every cluster replica must apply each metadata change into a
// durable BadgerDB store, not just single-node `serve`. The clustered
// metadata plane (internal/raftnode) mirrors every applied entry through a
// BadgerDB persister alongside the Raft WAL that backs recovery. The
// simulation harness cannot catch a regression here because it substitutes
// the WAL row-log persister for BadgerDB, so this test exercises the real
// production composition instead.
//
// It asserts the design: a metadata write on a running cluster node lands in
// a durable BadgerDB store on disk (1), and survives a real stop/start (2).
func TestClusterMetadataPersistsToBadgerDB(t *testing.T) {
	const (
		akid   = "e2e-persist"
		secret = "e2e-persist-secret"
		region = "us-east-1"
	)
	env := []string{"HAMSTER_ACCESS_KEY_ID=" + akid, "HAMSTER_SECRET_ACCESS_KEY=" + secret}
	dir := filepath.Join(t.TempDir(), "n1")
	s3 := freeAddr(t)

	// A one-node cluster — the simplest deployment that runs the *cluster*
	// metadata path (Raft + coordinator), as opposed to single-node `serve`.
	run(t, "init", "-data-dir", dir, "-cluster", "e2e-persist", "-node", "n1",
		"-listen", freeAddr(t))
	p := start(t, env, "serve", "-data-dir", dir, "-s3", s3)
	waitStatus(t, dir, "n1 leading alone", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})

	// Normal usage: create a bucket. Pure metadata — no object/EC path — so
	// this isolates *where metadata becomes durable*.
	c := &s3Client{t: t, akid: akid, secret: secret, region: region}
	c.mutate([]string{s3}, "PUT", "/persist-probe", nil, http.StatusOK)

	// (1) The design's claim: the write is now durable in BadgerDB on this
	// node. Today a cluster node has no BadgerDB store at all.
	if !hasBadgerStore(dir) {
		t.Fatalf("no BadgerDB store under %s after a metadata write — "+
			"cluster metadata is not persisted to BadgerDB (ADR-0005 gap)", dir)
	}

	// (2) Stop the node and start it again; the metadata must come back.
	p.interrupt(t)
	p = start(t, env, "serve", "-data-dir", dir, "-s3", s3)
	waitStatus(t, dir, "n1 leading after restart", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})
	// ListObjects on the bucket: 200 if it survived, 404 if the metadata
	// was lost. Retry while the S3 listener finishes coming up.
	deadline := time.Now().Add(30 * time.Second)
	survived := false
	for time.Now().Before(deadline) {
		if resp, _ := c.do(s3, "GET", "/persist-probe", nil); resp != nil && resp.StatusCode == http.StatusOK {
			survived = true
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !survived {
		t.Fatal("bucket persist-probe did not survive a stop/start")
	}
	p.interrupt(t)
}

// TestClusterMetadataRebuildsFromWALAfterStoreLoss is the durability answer to
// "what if the BadgerDB metadata store is lost or corrupt": the Raft WAL holds
// a complete copy (its snapshots are full metadata dumps), so a node whose
// durable store is gone rebuilds it from the log on restart — no peer, no data
// loss. Here the store is deleted between runs, the most total form of loss;
// an unreadable store funnels to the same rebuild (a hard open failure is
// discarded by the composition root, a read failure by the raftnode fallback).
func TestClusterMetadataRebuildsFromWALAfterStoreLoss(t *testing.T) {
	const (
		akid   = "e2e-rebuild"
		secret = "e2e-rebuild-secret"
		region = "us-east-1"
	)
	env := []string{"HAMSTER_ACCESS_KEY_ID=" + akid, "HAMSTER_SECRET_ACCESS_KEY=" + secret}
	dir := filepath.Join(t.TempDir(), "n1")
	s3 := freeAddr(t)

	run(t, "init", "-data-dir", dir, "-cluster", "e2e-rebuild", "-node", "n1",
		"-listen", freeAddr(t))
	p := start(t, env, "serve", "-data-dir", dir, "-s3", s3)
	waitStatus(t, dir, "n1 leading alone", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})

	c := &s3Client{t: t, akid: akid, secret: secret, region: region}
	c.mutate([]string{s3}, "PUT", "/rebuild-probe", nil, http.StatusOK)

	// Stop, destroy the durable metadata store, restart. The metadata lives
	// only in the Raft WAL now.
	p.interrupt(t)
	if err := os.RemoveAll(filepath.Join(dir, "meta")); err != nil {
		t.Fatalf("removing metadata store: %v", err)
	}
	p = start(t, env, "serve", "-data-dir", dir, "-s3", s3)
	waitStatus(t, dir, "n1 leading after restart", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})

	// The bucket must come back, rebuilt from the WAL, and the durable store
	// re-materialised.
	deadline := time.Now().Add(30 * time.Second)
	survived := false
	for time.Now().Before(deadline) {
		if resp, _ := c.do(s3, "GET", "/rebuild-probe", nil); resp != nil && resp.StatusCode == http.StatusOK {
			survived = true
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !survived {
		t.Fatal("bucket rebuild-probe did not survive loss of the metadata store")
	}
	if !hasBadgerStore(dir) {
		t.Fatalf("BadgerDB store was not re-materialised under %s after the rebuild", dir)
	}
	p.interrupt(t)
}

// hasBadgerStore reports whether a BadgerDB store exists anywhere under dir,
// detected by its on-disk signature: a MANIFEST plus a value log or SSTable.
func hasBadgerStore(dir string) bool {
	var manifest, data bool
	filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		switch name := d.Name(); {
		case name == "MANIFEST":
			manifest = true
		case strings.HasSuffix(name, ".vlog"), strings.HasSuffix(name, ".sst"):
			data = true
		}
		return nil
	})
	return manifest && data
}
