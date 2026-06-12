package sim_test

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/blob"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/sim"
	"github.com/hamster-storage/hamster/internal/wal"
)

// This file is the v0.1 sim-node integration (docs/SIMULATION.md, "Checking
// correctness", in its single-node degenerate form): a node composed of the
// real metadata store, the real blob store, and the WAL persister, booted
// from the simulated disk, crashed and restarted adversarially, and checked
// against a reference model after every recovery. The invariants exercised
// are the single-node cases of the durability and version-semantics checks:
//
//   - every acknowledged operation survives a crash, exactly;
//   - an operation interrupted before its metadata commit vanishes without
//     a trace in metadata (its blob may persist as an orphan — GC's job);
//   - every blob the metadata references exists and matches its checksum;
//   - multi-row transactions (multipart complete) are atomic across crashes.
//
// Everything runs on the simulator's single thread, so store calls are
// "on the loop" by construction — the same ownership discipline the
// gateway keeps with onLoop in production.

// storeNode is the node under test. It handles no messages yet: the
// network half of this composition arrives with Raft in v0.2.
type storeNode struct {
	w     *sim.World
	store *meta.Store
	blobs *blob.Store
}

func (*storeNode) HandleMessage(seam.NodeID, []byte) {}

func (n *storeNode) mint() meta.VersionID {
	return meta.NewVersionID(n.w.Clock.Now(), n.w.Rand)
}

func (n *storeNode) nowMS() int64 { return n.w.Clock.Now().UnixMilli() }

// model is the reference: what an object store that never crashes would
// hold after the acknowledged operations.
type model struct {
	buckets map[string]map[string][]byte // bucket → key → content
	uploads []*modelUpload
}

type modelUpload struct {
	bucket, key string
	uid         meta.VersionID
	planned     int
	parts       map[uint32][]byte
}

func TestSingleNodeCrashRecovery(t *testing.T) {
	for seed := range uint64(20) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runSingleNodeSim(t, seed)
		})
	}
}

func runSingleNodeSim(t *testing.T, seed uint64) {
	s := sim.New(seed, sim.NetConfig{})
	rng := rand.New(rand.NewPCG(seed, 99))

	var n *storeNode
	boot := func(w *sim.World) seam.MessageHandler {
		rowlog, rows, err := wal.OpenRows(w.Disk, "meta/log")
		if err != nil {
			t.Fatalf("boot: %v", err)
		}
		store := meta.NewStore()
		for k, v := range rows {
			if err := store.Restore(k, v); err != nil {
				t.Fatalf("boot restore: %v", err)
			}
		}
		store.SetPersister(rowlog)
		n = &storeNode{w: w, store: store, blobs: blob.NewStore(w.Disk)}
		return n
	}
	s.AddNode("n1", boot)

	m := &model{buckets: make(map[string]map[string][]byte)}
	crashRestart := func() {
		s.Crash("n1")
		s.Run(time.Second)
		s.Restart("n1")
		verify(t, n, m)
	}

	bucketNames := []string{"alpha", "beta", "gamma"}
	keyNames := []string{"a", "dir/b", "dir/c", "d.txt", "e", "f"}

	for step := 0; step < 120; step++ {
		s.Run(time.Second)
		switch r := rng.IntN(100); {
		case r < 8:
			crashRestart()
		case r < 18:
			opCreateBucket(t, n, m, rng, bucketNames)
		case r < 60:
			opPut(t, n, m, rng, crashRestart, bucketNames, keyNames)
		case r < 75:
			opDelete(t, n, m, rng, bucketNames, keyNames)
		default:
			opMultipart(t, n, m, rng, crashRestart, bucketNames, keyNames)
		}
	}
	// End of run: one last adversarial recovery, then the full check.
	crashRestart()
}

func opCreateBucket(t *testing.T, n *storeNode, m *model, rng *rand.Rand, names []string) {
	name := names[rng.IntN(len(names))]
	err := n.store.ApplyCreateBucket(meta.CreateBucket{ProposedAtUnixMS: n.nowMS(), Bucket: name})
	if _, exists := m.buckets[name]; exists {
		if err != meta.ErrBucketExists {
			t.Fatalf("recreating bucket %q: %v, want ErrBucketExists", name, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("creating bucket %q: %v", name, err)
	}
	m.buckets[name] = make(map[string][]byte)
}

// content builds a deterministic body: usually small, sometimes crossing
// the write buffer so the streamed path is exercised under simulation too.
func content(rng *rand.Rand) []byte {
	if rng.IntN(10) == 0 {
		return bytes.Repeat([]byte{byte(rng.Uint32()), 'x'}, 1<<19+rng.IntN(64)) // >1 MiB
	}
	b := make([]byte, rng.IntN(2048))
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	return b
}

// pickBucket returns a bucket that exists in the model, or "" if none do.
func pickBucket(m *model, rng *rand.Rand) string {
	if len(m.buckets) == 0 {
		return ""
	}
	names := slices.Sorted(maps.Keys(m.buckets))
	return names[rng.IntN(len(names))]
}

func opPut(t *testing.T, n *storeNode, m *model, rng *rand.Rand, crashRestart func(), buckets, keys []string) {
	bucket := pickBucket(m, rng)
	if bucket == "" {
		return
	}
	key := keys[rng.IntN(len(keys))]
	body := content(rng)

	// The gateway's discipline, exactly: blob durable first, metadata
	// commit second, displaced blobs reclaimed last.
	vid := n.mint()
	if _, err := n.blobs.Put(vid, bytes.NewReader(body)); err != nil {
		t.Fatalf("blob put: %v", err)
	}
	if rng.IntN(12) == 0 {
		// Crash in the gap: data durable, metadata never committed. The
		// operation was not acknowledged — the model must not change, and
		// the blob is an invisible orphan.
		crashRestart()
		return
	}

	displaced := currentDataIDs(n, bucket, key)
	sum := sha256.Sum256(body)
	etag := md5.Sum(body)
	if _, err := n.store.ApplyPutObject(meta.PutObject{
		ProposedAtUnixMS: n.nowMS(), Bucket: bucket, Key: key, VersionID: vid,
		Size: int64(len(body)), ETag: etag[:], ObjectChecksum: sum[:],
	}); err != nil {
		t.Fatalf("put %s/%s: %v", bucket, key, err)
	}
	m.buckets[bucket][key] = body

	if rng.IntN(10) == 0 {
		// Crash before reclaiming the displaced blobs: acknowledged write
		// survives, the stale blobs are orphans. Allowed, never visible.
		crashRestart()
		return
	}
	for _, id := range displaced {
		_ = n.blobs.Remove(id)
	}
}

func opDelete(t *testing.T, n *storeNode, m *model, rng *rand.Rand, buckets, keys []string) {
	bucket := pickBucket(m, rng)
	if bucket == "" {
		return
	}
	key := keys[rng.IntN(len(keys))]
	reclaim := currentDataIDs(n, bucket, key)
	res, err := n.store.ApplyDeleteObject(meta.DeleteObject{
		ProposedAtUnixMS: n.nowMS(), Bucket: bucket, Key: key, VersionID: n.mint(),
	})
	if err != nil {
		t.Fatalf("delete %s/%s: %v", bucket, key, err)
	}
	if res.Removed {
		for _, id := range reclaim {
			_ = n.blobs.Remove(id)
		}
	}
	delete(m.buckets[bucket], key)
}

// opMultipart advances the multipart state machine one step: start an
// upload, add a part, or finish (complete or abort) one that is ready.
func opMultipart(t *testing.T, n *storeNode, m *model, rng *rand.Rand, crashRestart func(), buckets, keys []string) {
	// Finish or feed an existing upload first; start a new one only when
	// fewer than two are in flight.
	for _, u := range m.uploads {
		if len(u.parts) < u.planned {
			opUploadPart(t, n, m, rng, crashRestart, u)
			return
		}
	}
	if len(m.uploads) > 0 && rng.IntN(2) == 0 {
		u := m.uploads[rng.IntN(len(m.uploads))]
		if rng.IntN(4) == 0 {
			opAbort(t, n, m, u)
		} else {
			opComplete(t, n, m, rng, u)
		}
		return
	}
	if len(m.uploads) >= 2 {
		return
	}
	bucket := pickBucket(m, rng)
	if bucket == "" {
		return
	}
	u := &modelUpload{
		bucket: bucket, key: keys[rng.IntN(len(keys))], uid: n.mint(),
		planned: 1 + rng.IntN(2), parts: make(map[uint32][]byte),
	}
	if err := n.store.ApplyCreateMultipartUpload(meta.CreateMultipartUpload{
		ProposedAtUnixMS: n.nowMS(), Bucket: u.bucket, Key: u.key, UploadID: u.uid,
	}); err != nil {
		t.Fatalf("create upload: %v", err)
	}
	m.uploads = append(m.uploads, u)
}

func opUploadPart(t *testing.T, n *storeNode, m *model, rng *rand.Rand, crashRestart func(), u *modelUpload) {
	num := uint32(len(u.parts) + 1)
	var body []byte
	if int(num) < u.planned {
		// Every part but the last must meet the 5 MiB floor at complete.
		body = bytes.Repeat([]byte{byte(num), 'p'}, meta.MinPartSize/2)
	} else {
		body = content(rng)
	}

	dataID := n.mint()
	if _, err := n.blobs.Put(dataID, bytes.NewReader(body)); err != nil {
		t.Fatalf("part blob put: %v", err)
	}
	if rng.IntN(12) == 0 {
		crashRestart() // the part was never committed; its blob is an orphan
		return
	}
	sum := sha256.Sum256(body)
	etag := md5.Sum(body)
	res, err := n.store.ApplyUploadPart(meta.UploadPart{
		ProposedAtUnixMS: n.nowMS(), Bucket: u.bucket, Key: u.key, UploadID: u.uid,
		PartNumber: num, DataID: dataID, Size: int64(len(body)), ETag: etag[:], Checksum: sum[:],
	})
	if err != nil {
		t.Fatalf("upload part %d: %v", num, err)
	}
	if !res.ReplacedDataID.IsZero() {
		_ = n.blobs.Remove(res.ReplacedDataID)
	}
	u.parts[num] = body
}

func opComplete(t *testing.T, n *storeNode, m *model, rng *rand.Rand, u *modelUpload) {
	nums := slices.Sorted(maps.Keys(u.parts))
	parts := make([]meta.CompletedPart, len(nums))
	md5s := make([]byte, 0, md5.Size*len(nums))
	var body []byte
	for i, num := range nums {
		etag := md5.Sum(u.parts[num])
		parts[i] = meta.CompletedPart{PartNumber: num, ETag: etag[:]}
		md5s = append(md5s, etag[:]...)
		body = append(body, u.parts[num]...)
	}
	composite := md5.Sum(md5s)

	displaced := currentDataIDs(n, u.bucket, u.key)
	res, err := n.store.ApplyCompleteMultipartUpload(meta.CompleteMultipartUpload{
		ProposedAtUnixMS: n.nowMS(), Bucket: u.bucket, Key: u.key, UploadID: u.uid,
		VersionID: n.mint(), ETag: composite[:], Parts: parts,
	})
	if err != nil {
		t.Fatalf("complete upload: %v", err)
	}
	for _, id := range append(displaced, res.DiscardedDataIDs...) {
		_ = n.blobs.Remove(id)
	}
	m.buckets[u.bucket][u.key] = body
	m.removeUpload(u)
}

func opAbort(t *testing.T, n *storeNode, m *model, u *modelUpload) {
	res, err := n.store.ApplyAbortMultipartUpload(meta.AbortMultipartUpload{
		ProposedAtUnixMS: n.nowMS(), Bucket: u.bucket, Key: u.key, UploadID: u.uid,
	})
	if err != nil {
		t.Fatalf("abort upload: %v", err)
	}
	for _, id := range res.PartDataIDs {
		_ = n.blobs.Remove(id)
	}
	m.removeUpload(u)
}

func (m *model) removeUpload(u *modelUpload) {
	m.uploads = slices.DeleteFunc(m.uploads, func(x *modelUpload) bool { return x == u })
}

// currentDataIDs mirrors the gateway's reclaim capture: the data addresses
// the next commit to this key displaces.
func currentDataIDs(n *storeNode, bucket, key string) []meta.VersionID {
	cur, ok := n.store.Current(bucket, key)
	if !ok {
		return nil
	}
	e, ok := n.store.GetVersion(bucket, key, cur.VersionID)
	if !ok {
		return nil
	}
	return e.DataIDs()
}

// verify checks the recovered node against the model: buckets, listings,
// every object's content through its checksums, every in-flight upload.
func verify(t *testing.T, n *storeNode, m *model) {
	t.Helper()

	var gotBuckets []string
	for _, b := range n.store.ListBuckets() {
		gotBuckets = append(gotBuckets, b.Name)
	}
	wantBuckets := slices.Sorted(maps.Keys(m.buckets))
	if !slices.Equal(gotBuckets, wantBuckets) {
		t.Fatalf("buckets after recovery: %v, want %v", gotBuckets, wantBuckets)
	}

	for bucket, objects := range m.buckets {
		var listed []string
		for _, o := range n.store.ListObjects(bucket, "", "", 10_000) {
			listed = append(listed, o.Key)
		}
		want := slices.Sorted(maps.Keys(objects))
		if !slices.Equal(listed, want) {
			t.Fatalf("listing of %q after recovery: %v, want %v", bucket, listed, want)
		}
		for key, body := range objects {
			cur, ok := n.store.Current(bucket, key)
			if !ok {
				t.Fatalf("acknowledged object %s/%s lost", bucket, key)
			}
			entry, ok := n.store.GetVersion(bucket, key, cur.VersionID)
			if !ok {
				t.Fatalf("current version of %s/%s dangles", bucket, key)
			}
			if entry.Size != int64(len(body)) {
				t.Fatalf("%s/%s size %d, want %d", bucket, key, entry.Size, len(body))
			}
			if got := readEntryData(t, n, entry); !bytes.Equal(got, body) {
				t.Fatalf("%s/%s content diverged after recovery (%d bytes, want %d)",
					bucket, key, len(got), len(body))
			}
		}
	}

	for _, u := range m.uploads {
		if _, ok := n.store.GetUpload(u.bucket, u.key, u.uid); !ok {
			t.Fatalf("in-flight upload %s/%s lost", u.bucket, u.key)
		}
		recs, _ := n.store.ListUploadParts(u.bucket, u.key, u.uid, 0, 10_000)
		if len(recs) != len(u.parts) {
			t.Fatalf("upload %s/%s has %d parts, want %d", u.bucket, u.key, len(recs), len(u.parts))
		}
		for _, rec := range recs {
			body, ok := u.parts[rec.PartNumber]
			if !ok || rec.Size != int64(len(body)) {
				t.Fatalf("upload %s/%s part %d diverged", u.bucket, u.key, rec.PartNumber)
			}
			blobMustMatch(t, n, rec.DataID, rec.Checksum, body)
		}
	}
}

// readEntryData mirrors the gateway's read path: fetch the entry's blobs
// and verify the stored checksums — metadata must never reference data the
// disk does not faithfully hold.
func readEntryData(t *testing.T, n *storeNode, entry meta.VersionEntry) []byte {
	t.Helper()
	if len(entry.Parts) > 0 {
		var data []byte
		for _, p := range entry.Parts {
			part, err := n.blobs.Get(p.DataID)
			if err != nil {
				t.Fatalf("metadata references missing part blob: %v", err)
			}
			if sum := sha256.Sum256(part); !bytes.Equal(sum[:], p.Checksum) {
				t.Fatal("part blob fails its checksum after recovery")
			}
			data = append(data, part...)
		}
		return data
	}
	data, err := n.blobs.Get(entry.DataID)
	if err != nil {
		t.Fatalf("metadata references missing blob: %v", err)
	}
	if sum := sha256.Sum256(data); !bytes.Equal(sum[:], entry.ObjectChecksum) {
		t.Fatal("blob fails its checksum after recovery")
	}
	return data
}

func blobMustMatch(t *testing.T, n *storeNode, id meta.VersionID, checksum, want []byte) {
	t.Helper()
	data, err := n.blobs.Get(id)
	if err != nil {
		t.Fatalf("metadata references missing blob: %v", err)
	}
	if sum := sha256.Sum256(data); !bytes.Equal(sum[:], checksum) || !bytes.Equal(data, want) {
		t.Fatal("blob diverged from its checksum or the model after recovery")
	}
}
