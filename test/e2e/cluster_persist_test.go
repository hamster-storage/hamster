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
// for an ADR-0005 implementation gap. The design (ADR-0005, METADATA.md) is
// that every cluster replica applies each metadata change into BadgerDB. In
// fact only single-node `serve` wires BadgerDB; the clustered metadata plane
// (internal/raftnode) attaches no persister and recovers from the Raft
// write-ahead log plus snapshots, so a cluster node never writes BadgerDB at
// all. The simulation harness never caught this because it substitutes the
// WAL row-log persister for BadgerDB; this is the kind of thing only a test
// of the real production composition surfaces.
//
// The test asserts the design: a metadata write on a running cluster node
// lands in a durable BadgerDB store on disk (1), and survives a real
// stop/start (2). It is SKIPPED until the gap is closed — remove the skip to
// drive the fix red→green.
func TestClusterMetadataPersistsToBadgerDB(t *testing.T) {
	t.Skip("RED: cluster metadata is not persisted to BadgerDB — ADR-0005 gap; remove this skip to drive the fix")

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
	run(t, "cluster", "init", "-data-dir", dir, "-cluster", "e2e-persist", "-node", "n1",
		"-listen-cluster", freeAddr(t), "-listen-join", freeAddr(t))
	p := start(t, env, "cluster", "run", "-data-dir", dir, "-s3", s3)
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
	p = start(t, env, "cluster", "run", "-data-dir", dir, "-s3", s3)
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
