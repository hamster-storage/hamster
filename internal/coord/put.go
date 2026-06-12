package coord

import (
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/hamster-storage/hamster/internal/datapath"
	"github.com/hamster-storage/hamster/internal/ec"
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

// Put stores one object: erasure-code the body onto the partition's
// nodes, enforce the ack rule, commit the metadata through Raft, and call
// done exactly once on the loop. The body slice must not be mutated until
// done fires.
func (c *Coordinator) Put(bucket, key string, body []byte, done func(PutResult, error)) {
	now := c.cfg.Clock.Now()
	vid := meta.NewVersionID(now, c.cfg.Rand)
	size := int64(len(body))
	k, m := c.cfg.Profile.Params(size)

	partition := place.Partition(vid, c.cfg.PartitionCount)
	nodes, err := place.Nodes(partition, c.cfg.Members, k+m)
	if err != nil {
		done(PutResult{}, fmt.Errorf("coord: placing %d shards: %w", k+m, err))
		return
	}

	etag := md5.Sum(body)
	objSum := sha256.Sum256(body)
	op := &putOp{
		c: c, done: done,
		bucket: bucket, key: key, atMS: now.UnixMilli(),
		vid: vid, body: body, k: k, m: m,
		floor:     min(k+1, k+m),
		partition: partition,
		nodes:     nodes,
		etag:      etag[:], objSum: objSum[:],
		failed: make([]bool, k+m),
	}

	op.streams = make([]*datapath.WriteStream, k+m)
	op.sinks = make([]*sink, k+m)
	for i := range op.streams {
		i := i
		op.streams[i] = c.cfg.Data.NewWrite(nodes[i], vid, uint32(i),
			func() { op.step() },
			func(err error) { op.streamDone(i, err) })
		op.sinks[i] = &sink{ws: op.streams[i]}
	}

	sinks := make([]io.Writer, k+m)
	for i, s := range op.sinks {
		sinks[i] = s
	}
	frameSize := stream.FrameSize(size, stream.DefaultChunkSize)
	ecw, err := ec.NewWriter(vid, k, m, frameSize, sinks)
	if err != nil {
		op.abort(fmt.Errorf("coord: encoder: %w", err))
		return
	}
	sw, err := stream.NewWriter(ecw, size, stream.DefaultChunkSize)
	if err != nil {
		op.abort(fmt.Errorf("coord: framing: %w", err))
		return
	}
	op.ecw, op.sw = ecw, sw
	op.step()
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
	s.ws.Write(p)
	s.n += int64(len(p))
	return len(p), nil
}

// putOp is one in-flight PUT: feed the body as windows allow, close and
// commit, gather stream outcomes, apply the ack rule, propose.
type putOp struct {
	c    *Coordinator
	done func(PutResult, error)

	bucket, key string
	atMS        int64
	vid         meta.VersionID
	body        []byte
	k, m        int
	floor       int
	partition   uint64
	nodes       []seam.NodeID
	etag        []byte
	objSum      []byte

	streams []*datapath.WriteStream
	sinks   []*sink
	ecw     *ec.Writer
	sw      *stream.Writer

	fed       int
	closed    bool
	failed    []bool
	failures  int
	successes int
	finished  bool
}

// step advances the feed whenever window opens: write the next stripe's
// worth of body when every live stream can absorb its slice burst, then
// close and commit when the body is fully fed.
func (op *putOp) step() {
	if op.finished || op.closed {
		return
	}
	stripeBytes := op.k * ec.DefaultSliceSize
	stepBytes := max(stripeBytes-4096, 1)

	for op.fed < len(op.body) {
		if !op.windowsAllow(stepNeed) {
			return // an ack or a stream failure will call step again
		}
		n := min(stepBytes, len(op.body)-op.fed)
		if _, err := op.sw.Write(op.body[op.fed : op.fed+n]); err != nil {
			op.abort(fmt.Errorf("coord: framing body: %w", err))
			return
		}
		op.fed += n
	}

	if !op.windowsAllow(closeNeed) {
		return
	}
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
// the metadata commit — the linearization point — and acknowledges.
func (op *putOp) evaluate(lastErr error) {
	if op.successes < op.floor || !op.closed {
		op.fail(fmt.Errorf("%w (%d of %d shards durable, floor %d): %v",
			ErrRefused, op.successes, len(op.streams), op.floor, lastErr))
		return
	}
	op.finished = true
	op.c.cfg.Raft.Propose(meta.PutObject{
		ProposedAtUnixMS: op.atMS,
		Bucket:           op.bucket,
		Key:              op.key,
		VersionID:        op.vid,
		Size:             int64(len(op.body)),
		ETag:             op.etag,
		Partition:        op.partition,
		ECDataShards:     uint32(op.k),
		ECParityShards:   uint32(op.m),
		ObjectChecksum:   op.objSum,
		ShardChecksums:   op.ecw.Checksums(),
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
