package coord

import (
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/hamster-storage/hamster/internal/datapath"
	"github.com/hamster-storage/hamster/internal/ec"
	"github.com/hamster-storage/hamster/internal/keys"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/place"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/stream"
)

// Pacing bounds. A feed step writes just under one stripe of body, but
// the frame writer buffers up to a whole chunk before flushing, so one
// step can complete up to two stripes inside the encoder — a burst of
// two slices per sink — and the close (final chunk flush, frame trailer,
// final-stripe padding) up to three. The window checks demand the worst
// case plus slack, so a paced write can never overrun a stream's window.
const (
	stepNeed  = 2*ec.DefaultSliceSize + 8<<10
	closeNeed = 3*ec.DefaultSliceSize + 64<<10
)

// ErrRefused is the ack rule refusing a write below the durability floor
// — the S3 SlowDown/503 family: try again when the cluster is healthier,
// because an acknowledgment here would be a durability lie (ADR-0015).
var ErrRefused = fmt.Errorf("coord: too few nodes reachable to write durably (SlowDown)")

// PutOptions carries the S3 request facts the committed version records.
type PutOptions struct {
	ContentType  string
	UserMetadata map[string]string
	// Object-lock fields (ADR-0006), set from the x-amz-object-lock-* headers.
	// Zero values mean no per-object lock; a bucket default retention is applied
	// in the metadata layer, so it needs nothing here.
	RetentionMode     meta.RetentionMode
	RetainUntilUnixMS int64
	LegalHold         bool
}

// defaultPutChunkBytes is the unit the streaming feeder pushes;
// defaultPutMaxOutstanding bounds how many chunks may be requested-but-not-yet-
// encoded at once. Together they cap a streaming PUT's body buffer at a few MiB
// regardless of object size. Config.PutChunkBytes/PutMaxOutstanding override
// them for load tuning.
const (
	defaultPutChunkBytes     = 1 << 20
	defaultPutMaxOutstanding = 2
)

// putWindow returns the backpressure window, applying the defaults when the
// config leaves a knob at zero.
func (c *Coordinator) putWindow() (chunkBytes, maxOut int) {
	chunkBytes, maxOut = c.cfg.PutChunkBytes, c.cfg.PutMaxOutstanding
	if chunkBytes <= 0 {
		chunkBytes = defaultPutChunkBytes
	}
	if maxOut <= 0 {
		maxOut = defaultPutMaxOutstanding
	}
	return chunkBytes, maxOut
}

// PutChunkSize is the chunk size the streaming feeder should read — the
// configured window's unit. Exposed so the gateway feeder and the coordinator
// agree on the chunk granularity.
func (c *Coordinator) PutChunkSize() int {
	chunkBytes, _ := c.putWindow()
	return chunkBytes
}

// PutMaxOutstanding is the most chunk requests the coordinator issues before a
// Feed — the feeder sizes its want channel to it so the want callback never
// blocks the loop.
func (c *Coordinator) PutMaxOutstanding() int {
	_, maxOut := c.putWindow()
	return maxOut
}

// Put stores one whole in-memory object — the convenience entry point over the
// streaming machinery for callers that already hold the body. It feeds the body
// whole and closes; the loop-paced encode and the ack rule are identical to a
// streamed PUT. The body slice must not be mutated until done fires. done fires
// exactly once on the loop.
func (c *Coordinator) Put(bucket, key string, body []byte, opts PutOptions, done func(PutResult, error)) {
	op := c.beginPut(bucket, key, int64(len(body)), opts, nil, done)
	if op == nil {
		return
	}
	op.Feed(body)
	op.FeedEOF()
}

// PutHandle drives a streaming PUT: the caller feeds body chunks as they arrive
// and closes at EOF. Feed and FeedEOF run on the loop (the caller posts them);
// the coordinator calls the want callback on the loop when it has room for
// another chunk, so the caller reads the next chunk off-loop only when asked —
// bounded memory with end-to-end backpressure.
type PutHandle struct{ op *putOp }

// Feed appends a body chunk. Call on the loop.
func (h *PutHandle) Feed(chunk []byte) { h.op.Feed(chunk) }

// FeedEOF marks the body complete, triggering close and the metadata commit.
// Call on the loop, once.
func (h *PutHandle) FeedEOF() { h.op.FeedEOF() }

// Abort cancels an in-flight streaming PUT — for a feeder that hit a read or
// validation error. The staged shards are reclaimed and done fires with err.
// Call on the loop.
func (h *PutHandle) Abort(err error) { h.op.abort(err) }

// PutStream begins a streaming PUT of size bytes. want fires on the loop each
// time the coordinator can accept another chunk (the caller feeds via the
// handle). done fires exactly once on the loop. A nil handle means setup failed
// and done has already fired.
func (c *Coordinator) PutStream(bucket, key string, size int64, opts PutOptions, want func(), done func(PutResult, error)) *PutHandle {
	op := c.beginPut(bucket, key, size, opts, want, done)
	if op == nil {
		return nil
	}
	op.replenish()
	return &PutHandle{op}
}

// beginPut resolves placement, the storage profile, and the encryption posture,
// then opens the k+m shard write streams and the encoder for an object of size
// bytes. It returns the in-flight op, or nil after firing done on a setup
// failure. The caller feeds the body next: Put feeds it whole, PutStream feeds
// it in chunks under backpressure. The body's plaintext MD5 (ETag) and SHA-256
// (object checksum) are accumulated as it is fed and finalized at close.
func (c *Coordinator) beginPut(bucket, key string, size int64, opts PutOptions, want func(), done func(PutResult, error)) *putOp {
	now := c.cfg.Clock.Now()
	// Time the write from admission to completion through the seam clock
	// (ADR-0039 part 1): observe the service time only on the success terminal —
	// a refused or failed PUT is not a latency sample. Wrapping done here covers
	// every terminal path (commit success, abort, fail) and both commit
	// strategies (a whole PutObject and a multipart UploadPart, both writes).
	userDone := done
	done = func(r PutResult, err error) {
		if err == nil {
			c.observeLatency(opPut, now)
		}
		userDone(r, err)
	}
	vid := meta.NewVersionID(now, c.cfg.Rand)

	layout, ok := c.cfg.Layout()
	if !ok {
		// No layout installed yet — the cluster is still forming. Refuse
		// transiently, the same SlowDown a write below the floor gets.
		done(PutResult{}, fmt.Errorf("coord: no cluster layout yet: %w", ErrRefused))
		return nil
	}
	// The profile follows the active (non-draining) node count, so a write
	// during a downsize already lands at the target profile the shrink converges
	// to — and a same-size drain (the active count unchanged) writes exactly as
	// before (ADR-0004, ADR-0015).
	k, m := ec.AutoProfile(layout.ActiveCount()).Params(size)

	partition := place.Partition(vid, layout.PartitionCount)
	nodes, err := layout.Nodes(partition, k+m)
	if err != nil {
		done(PutResult{}, fmt.Errorf("coord: placing %d shards: %w", k+m, err))
		return nil
	}
	floor := min(k+1, k+m)

	// Skip nodes the liveness detector considers down — but only while enough
	// remain up to still meet the durability floor. Skipping spares a
	// known-down node's retransmit timeout up front, and repair rebuilds its
	// shard when it returns. If too many look down to meet the floor, attempt
	// them all anyway: the marks may be stale, and the ack rule refuses
	// honestly if they are not — never a pre-emptive false refusal.
	skip := make([]bool, k+m)
	up := k + m
	for i, id := range nodes {
		if c.liveness.isDown(id, now) {
			skip[i] = true
			up--
		}
	}
	if up < floor {
		for i := range skip {
			skip[i] = false
		}
	}

	// Resolve the encryption posture for this write (ADR-0021). When on, mint
	// a per-object DEK and wrap it under the node's KEK now: the wrapped DEK
	// rides the metadata commit, while the raw DEK drives the stream transform
	// and is dropped when the frame writer has it. The wrap nonce is the
	// version ID — unique and never reused, so no nonce collision ever.
	kek, encOn := c.encryption()
	var (
		encAlg     meta.EncAlgorithm
		wrappedDEK []byte
		dekBytes   []byte
		kekFP      uint64
	)
	if encOn {
		if !kek.Loaded() {
			done(PutResult{}, fmt.Errorf("coord: encryption enabled but no KEK loaded: %w", ErrRefused))
			return nil
		}
		kekFP = kek.Fingerprint().Uint64()
		// Write-time KEK guard (ADR-0032): once the cluster has established its
		// current KEK fingerprint, a node whose loaded key does not match it (nor
		// the key a rotation is moving to) holds the wrong master key — refuse
		// rather than write an object no other node could read. Skipped while the
		// fingerprint is unestablished (a fresh or just-upgraded cluster).
		post := c.cfg.Raft.Store().EncryptionPosture()
		if post.CurrentKEKFingerprint != 0 &&
			kekFP != post.CurrentKEKFingerprint && kekFP != post.RotatingToKEKFingerprint {
			done(PutResult{}, fmt.Errorf("coord: loaded KEK %016x is not the cluster key: %w", kekFP, ErrRefused))
			return nil
		}
		dek, err := keys.NewDEK(c.cfg.Entropy)
		if err != nil {
			done(PutResult{}, fmt.Errorf("coord: minting DEK: %w", err))
			return nil
		}
		if wrappedDEK, err = kek.Wrap(dek, wrapNonce(vid)); err != nil {
			done(PutResult{}, fmt.Errorf("coord: wrapping DEK: %w", err))
			return nil
		}
		encAlg, dekBytes = meta.EncAES256GCM, dek.Bytes()
	}

	chunkBytes, maxOut := c.putWindow()
	op := &putOp{
		c: c, done: done, opts: opts, want: want,
		bucket: bucket, key: key, atMS: now.UnixMilli(),
		vid: vid, size: size, k: k, m: m,
		floor:      floor,
		partition:  partition,
		nodes:      nodes,
		md5h:       md5.New(),
		shah:       sha256.New(),
		chunkBytes: chunkBytes, maxOut: maxOut,
		encAlg: encAlg, wrappedDEK: wrappedDEK, kekFP: kekFP,
		failed: make([]bool, k+m),
	}
	// The whole-object commit by default; PutPart swaps in the part commit
	// before feeding. Either way the streaming, pacing, and ack machinery below
	// is identical — only the metadata proposal at the linearization point and
	// the acknowledged result differ.
	op.commit = op.commitPutObject

	op.streams = make([]*datapath.WriteStream, k+m)
	op.sinks = make([]*sink, k+m)
	for i := range op.streams {
		i := i
		if skip[i] {
			// A known-down node: pre-fail its shard rather than open a stream
			// that would only time out. The sink swallows the encoder's bytes
			// for this position; the ack rule and pacing only consult live
			// streams, and repair places the shard when the node returns.
			op.failed[i] = true
			op.failures++
			op.sinks[i] = &sink{}
			continue
		}
		op.streams[i] = c.cfg.Data.NewWrite(nodes[i], vid, uint32(i),
			func() { op.step() },
			func(err error) { op.streamDone(i, err) })
		op.sinks[i] = &sink{ws: op.streams[i]}
	}

	sinks := make([]io.Writer, k+m)
	for i, s := range op.sinks {
		sinks[i] = s
	}
	frameSize := stream.FrameSize(size, stream.DefaultChunkSize, encOn)
	ecw, err := ec.NewWriter(vid, k, m, frameSize, sinks)
	if err != nil {
		op.abort(fmt.Errorf("coord: encoder: %w", err))
		return nil
	}
	sw, err := stream.NewWriter(ecw, size, stream.DefaultChunkSize, dekBytes)
	if err != nil {
		op.abort(fmt.Errorf("coord: framing: %w", err))
		return nil
	}
	op.ecw, op.sw = ecw, sw
	return op
}

// Feed appends a plaintext body chunk: hash it for the ETag and the object
// checksum, buffer it, and pace what the stream windows allow into the encoder.
// Call on the loop.
func (op *putOp) Feed(chunk []byte) {
	if op.finished {
		return
	}
	op.md5h.Write(chunk)
	op.shah.Write(chunk)
	op.pending = append(op.pending, chunk...)
	if op.outstanding > 0 {
		op.outstanding--
	}
	op.step()
}

// FeedEOF marks the body complete; step closes the frame and commits once the
// buffered tail has drained into the encoder. Call on the loop, once.
func (op *putOp) FeedEOF() {
	if op.finished {
		return
	}
	op.eofSeen = true
	op.step()
}

// replenish asks the feeder for more chunks while the buffer has room and the
// body is not yet complete — the backpressure that bounds memory. A nil want
// (the in-memory Put) supplies the whole body up front, so there is nothing to
// request.
func (op *putOp) replenish() {
	if op.want == nil || op.eofSeen {
		return
	}
	for op.outstanding < op.maxOut && len(op.pending) < op.maxOut*op.chunkBytes {
		op.outstanding++
		op.want()
	}
}

// sink adapts a WriteStream to the io.Writer the erasure coder pushes
// into, counting bytes (the commit needs the shard file length). A failed
// stream swallows writes — its shard is already lost to this PUT, and the
// coordinator's pacing only consults live streams.
type sink struct {
	ws *datapath.WriteStream
	n  int64
}

func (s *sink) Write(p []byte) (int, error) {
	if s.ws != nil { // nil on a skipped (known-down) shard: swallow the bytes
		s.ws.Write(p)
	}
	s.n += int64(len(p))
	return len(p), nil
}

// putOp is one in-flight PUT: feed the body as windows allow, close and
// commit, gather stream outcomes, apply the ack rule, propose.
type putOp struct {
	c      *Coordinator
	done   func(PutResult, error)
	commit func() // the success path: build the metadata proposal and ack (PutObject, or UploadPart for a part)
	want   func() // backpressure: ask the feeder for another chunk (nil for in-memory Put)

	bucket, key string
	opts        PutOptions
	atMS        int64
	vid         meta.VersionID
	size        int64
	k, m        int
	floor       int
	partition   uint64
	nodes       []seam.NodeID
	md5h        hash.Hash // plaintext MD5 → ETag, finalized at close
	shah        hash.Hash // plaintext SHA-256 → object checksum, finalized at close
	etag        []byte
	objSum      []byte
	encAlg      meta.EncAlgorithm
	wrappedDEK  []byte
	kekFP       uint64

	streams []*datapath.WriteStream
	sinks   []*sink
	ecw     *ec.Writer
	sw      *stream.Writer

	pending     []byte // body fed but not yet paced into the encoder
	outstanding int    // chunks requested from the feeder but not yet received
	chunkBytes  int    // backpressure window: feeder chunk size
	maxOut      int    // backpressure window: max chunks outstanding
	eofSeen     bool
	closed      bool
	failed      []bool
	failures    int
	successes   int
	finished    bool
}

// step paces buffered body into the encoder whenever window opens: write the
// next stripe's worth when every live stream can absorb its slice burst. When
// the buffer drains and more body is coming, ask the feeder for it; when the
// buffer drains and the body is complete, close and commit.
func (op *putOp) step() {
	if op.finished || op.closed {
		return
	}
	stripeBytes := op.k * ec.DefaultSliceSize
	stepBytes := max(stripeBytes-4096, 1)

	for len(op.pending) > 0 {
		if !op.windowsAllow(stepNeed) {
			return // an ack or a stream failure will call step again
		}
		n := min(stepBytes, len(op.pending))
		if _, err := op.sw.Write(op.pending[:n]); err != nil {
			op.abort(fmt.Errorf("coord: framing body: %w", err))
			return
		}
		op.pending = op.pending[n:]
	}

	// The buffer is drained. If more body is coming, ask for it and wait for
	// the next Feed; the close can only run once the whole body is in.
	if !op.eofSeen {
		op.replenish()
		return
	}

	if !op.windowsAllow(closeNeed) {
		return
	}
	op.etag = op.md5h.Sum(nil)
	op.objSum = op.shah.Sum(nil)
	if err := op.sw.Close(); err != nil {
		op.abort(fmt.Errorf("coord: closing frame: %w", err))
		return
	}
	if err := op.ecw.Close(); err != nil {
		op.abort(fmt.Errorf("coord: closing encoder: %w", err))
		return
	}
	op.closed = true
	checksums := op.ecw.Checksums()
	for i, ws := range op.streams {
		if !op.failed[i] {
			ws.Commit(op.sinks[i].n, checksums[i])
		}
	}
}

// windowsAllow reports whether every live stream can absorb need bytes.
// Failed streams swallow writes and never constrain pacing; if every
// stream has failed the abort path has already run.
func (op *putOp) windowsAllow(need int) bool {
	for i, ws := range op.streams {
		if !op.failed[i] && ws.Window() < need {
			return false
		}
	}
	return true
}

// streamDone records one shard outcome. Failures beyond what the floor
// tolerates abort immediately; otherwise the ack rule runs once every
// stream has reported.
func (op *putOp) streamDone(i int, err error) {
	if op.finished {
		return
	}
	// Fold the outcome into the liveness detector: a write that timed out
	// marks the node down (later PUTs skip it), a success clears it, a
	// receiver that answered with an error leaves the view unchanged (it is up,
	// just unable). Skipped shards never reach here, so their down mark
	// persists until it lapses.
	op.c.observe(op.nodes[i], err)
	if err != nil {
		op.failed[i] = true
		op.failures++
		if op.successes+op.failures < len(op.streams) && op.failures > len(op.streams)-op.floor {
			op.abort(fmt.Errorf("%w (%d of %d shard writes failed, floor %d): last: %v",
				ErrRefused, op.failures, len(op.streams), op.floor, err))
			return
		}
	} else {
		op.successes++
	}
	if op.successes+op.failures < len(op.streams) {
		op.step() // a failure may have unblocked pacing
		return
	}
	op.evaluate(err)
}

// evaluate applies the ack rule after every stream reported, then makes
// the metadata commit — the linearization point — and acknowledges via the
// op's commit strategy (a whole PutObject, or an UploadPart for a part).
func (op *putOp) evaluate(lastErr error) {
	if op.successes < op.floor || !op.closed {
		op.fail(fmt.Errorf("%w (%d of %d shards durable, floor %d): %v",
			ErrRefused, op.successes, len(op.streams), op.floor, lastErr))
		return
	}
	op.finished = true
	op.commit()
}

// commitPutObject proposes the whole-object PutObject and acknowledges — the
// default commit strategy. The committed shards become readable only when this
// proposal applies; a commit failure leaves them as reclaimable orphans.
func (op *putOp) commitPutObject() {
	op.c.cfg.Raft.Propose(meta.PutObject{
		ProposedAtUnixMS:  op.atMS,
		Bucket:            op.bucket,
		Key:               op.key,
		VersionID:         op.vid,
		Size:              op.size,
		ETag:              op.etag,
		ContentType:       op.opts.ContentType,
		UserMetadata:      op.opts.UserMetadata,
		Partition:         op.partition,
		ECDataShards:      uint32(op.k),
		ECParityShards:    uint32(op.m),
		ObjectChecksum:    op.objSum,
		ShardChecksums:    op.ecw.Checksums(),
		RetentionMode:     op.opts.RetentionMode,
		RetainUntilUnixMS: op.opts.RetainUntilUnixMS,
		LegalHold:         op.opts.LegalHold,
		EncAlgorithm:      op.encAlg,
		WrappedDEK:        op.wrappedDEK,
		KEKFingerprint:    op.kekFP,
	}, func(res any, err error) {
		if err != nil {
			// Durable shards without metadata are orphans; reclaim what
			// answers, the rest is markerless or scan-discoverable garbage.
			op.cleanup()
			op.done(PutResult{}, fmt.Errorf("coord: metadata commit: %w", err))
			return
		}
		op.done(PutResult{
			VersionID: res.(meta.PutResult).VersionID,
			ETag:      op.etag,
			Durable:   op.successes,
		}, nil)
	})
}

// abort is the early exit: stop every live stream, reclaim, fail.
func (op *putOp) abort(err error) {
	if op.finished {
		return
	}
	op.finished = true
	for i, ws := range op.streams {
		if !op.failed[i] {
			ws.Abort() // fires streamDone, which sees finished and returns
		}
	}
	op.cleanup()
	op.done(PutResult{}, err)
}

// fail is abort after all streams already reported.
func (op *putOp) fail(err error) {
	op.finished = true
	op.cleanup()
	op.done(PutResult{}, err)
}

// cleanup best-effort deletes whatever this PUT committed to disk on the
// targets. Outcomes are ignored: a shard that survives is markerless
// staging garbage or an orphan a future scan reclaims — never readable
// as an object, because no metadata names it.
func (op *putOp) cleanup() {
	for i := range op.streams {
		op.c.cfg.Data.Delete(op.nodes[i], op.vid, uint32(i), func(error) {})
	}
}
