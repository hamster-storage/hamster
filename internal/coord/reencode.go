package coord

import (
	"bytes"
	"fmt"
	"io"

	"github.com/hamster-storage/hamster/internal/datapath"
	"github.com/hamster-storage/hamster/internal/ec"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
)

// ReEncode rewrites one committed version's data to a new storage profile
// (ADR-0004, ADR-0015): read the object through its current shards, re-encode
// the framed stream at the new k+m, write the new shards to their placement,
// commit the metadata switch, and drop the old shards. A physical
// re-representation — the bytes, and so the object's identity, are unchanged.
// done fires once on the loop.
//
// The new shards are durable before the commit and the old ones drop only after
// it, so the object stays readable throughout: at the old profile until the
// commit, at the new one after. Any failure before the commit leaves the version
// exactly as it was. Used to step data down to a smaller profile as a cluster
// shrinks (and up as it grows).
func (c *Coordinator) ReEncode(bucket, key string, entry meta.VersionEntry, newK, newM int, done func(error)) {
	if entry.Kind != meta.KindObject || len(entry.Parts) > 0 {
		done(fmt.Errorf("coord: re-encode: not a whole object"))
		return
	}
	layout, ok := c.cfg.Layout()
	if !ok {
		done(fmt.Errorf("coord: re-encode: no cluster layout"))
		return
	}
	oldK, oldM := int(entry.ECDataShards), int(entry.ECParityShards)
	// The object's current shards sit at its current-width placement. During a
	// transition (a downsize opens one) the un-migrated object is at its old
	// home, so resolve through Locate and read from there; Nodes alone would be
	// the post-drain ordering, where the shards are not (ADR-0004).
	readNew, readOld, err := layout.Locate(entry.Partition, oldK+oldM)
	if err != nil {
		done(fmt.Errorf("coord: re-encode placing old: %w", err))
		return
	}
	oldNodes := readNew
	if readOld != nil {
		oldNodes = readOld
	}
	newNodes, err := layout.Nodes(entry.Partition, newK+newM)
	if err != nil {
		done(fmt.Errorf("coord: re-encode placing new: %w", err))
		return
	}
	op := &reencodeOp{
		c: c, done: done, bucket: bucket, key: key,
		atMS:  c.cfg.Clock.Now().UnixMilli(),
		entry: entry,
		oldK:  oldK, oldM: oldM, oldNodes: oldNodes,
		newK: newK, newM: newM, newNodes: newNodes,
		newDID: meta.NewVersionID(c.cfg.Clock.Now(), c.cfg.Rand),
	}
	op.probe()
}

type reencodeOp struct {
	c    *Coordinator
	done func(error)

	bucket, key string
	atMS        int64
	entry       meta.VersionEntry

	oldK, oldM int
	oldNodes   []seam.NodeID
	newK, newM int
	newNodes   []seam.NodeID
	newDID     meta.VersionID

	lengths []int64 // old shard length, -1 if absent
	pending int
	failed  bool

	newChecksums [][]byte
	writesLeft   int
	writeErr     error
	finished     bool
}

func (op *reencodeOp) oldWidth() int { return op.oldK + op.oldM }
func (op *reencodeOp) newWidth() int { return op.newK + op.newM }

// probe verifies every old shard to learn which are present and how long, so the
// fetch can pull whole shards.
func (op *reencodeOp) probe() {
	op.lengths = make([]int64, op.oldWidth())
	op.pending = op.oldWidth()
	for i := range op.oldNodes {
		i := i
		op.lengths[i] = -1
		op.c.cfg.Data.Verify(op.oldNodes[i], op.entry.DataID, uint32(i), func(r datapath.VerifyResult, err error) {
			op.c.observe(op.oldNodes[i], err)
			if err == nil && r.Committed {
				op.lengths[i] = r.Length
			}
			op.pending--
			if op.pending == 0 {
				op.fetch()
			}
		})
	}
}

// fetch pulls k present shards whole — enough to reconstruct the framed stream.
func (op *reencodeOp) fetch() {
	buffers := make(map[int][]byte)
	type piece struct {
		i           int
		off, length int64
	}
	var pieces []piece
	got := 0
	for i, l := range op.lengths {
		if l < 0 || got == op.oldK {
			continue
		}
		got++
		buffers[i] = make([]byte, l)
		for o := int64(0); o < l; o += fetchPiece {
			pieces = append(pieces, piece{i, o, min(int64(fetchPiece), l-o)})
		}
	}
	if got < op.oldK {
		op.finish(fmt.Errorf("coord: re-encode: %d of %d shards available, need %d to read", got, op.oldWidth(), op.oldK))
		return
	}
	if len(pieces) == 0 {
		op.encode(buffers)
		return
	}
	op.pending = len(pieces)
	for _, p := range pieces {
		p := p
		op.c.cfg.Data.Fetch(op.oldNodes[p.i], op.entry.DataID, uint32(p.i), p.off, int(p.length), func(b []byte, err error) {
			op.c.observe(op.oldNodes[p.i], err)
			if err != nil || int64(len(b)) != p.length {
				op.failed = true
			} else {
				copy(buffers[p.i][p.off:], b)
			}
			op.pending--
			if op.pending == 0 {
				if op.failed {
					op.finish(fmt.Errorf("coord: re-encode: fetching source shards"))
					return
				}
				op.encode(buffers)
			}
		})
	}
}

// encode reconstructs the framed stream from the fetched shards and re-encodes
// it at the new profile into in-memory buffers — pure computation, the shard
// CRCs verified on the way in.
func (op *reencodeOp) encode(buffers map[int][]byte) {
	shards := make([]io.ReaderAt, op.oldWidth())
	for i, b := range buffers {
		shards[i] = bytes.NewReader(b)
	}
	er, err := ec.NewReader(shards)
	if err != nil {
		op.finish(fmt.Errorf("coord: re-encode: opening source: %w", err))
		return
	}
	frameSize := er.FrameSize()

	bufs := make([]*bytes.Buffer, op.newWidth())
	sinks := make([]io.Writer, op.newWidth())
	for i := range bufs {
		bufs[i] = &bytes.Buffer{}
		sinks[i] = bufs[i]
	}
	ecw, err := ec.NewWriter(op.newDID, op.newK, op.newM, frameSize, sinks)
	if err != nil {
		op.finish(fmt.Errorf("coord: re-encode: encoder: %w", err))
		return
	}
	if _, err := io.Copy(ecw, io.NewSectionReader(er, 0, frameSize)); err != nil {
		op.finish(fmt.Errorf("coord: re-encode: re-encoding: %w", err))
		return
	}
	if err := ecw.Close(); err != nil {
		op.finish(fmt.Errorf("coord: re-encode: closing encoder: %w", err))
		return
	}
	op.newChecksums = ecw.Checksums()
	op.write(bufs)
}

// write streams each new shard to its placement, requiring every one durable:
// the old shards are still the readable copy, so a partial re-encode aborts and
// leaves the version untouched rather than committing a degraded one.
func (op *reencodeOp) write(bufs []*bytes.Buffer) {
	op.writesLeft = op.newWidth()
	for i := range bufs {
		i := i
		data := bufs[i].Bytes()
		var ws *datapath.WriteStream
		pos := 0
		feed := func() {
			for pos < len(data) && ws.Window() > 0 {
				end := min(pos+ws.Window(), len(data))
				ws.Write(data[pos:end])
				pos = end
			}
			if pos == len(data) {
				ws.Commit(int64(len(data)), op.newChecksums[i])
			}
		}
		ws = op.c.cfg.Data.NewWrite(op.newNodes[i], op.newDID, uint32(i), func() { feed() }, func(err error) {
			op.c.observe(op.newNodes[i], err)
			if err != nil && op.writeErr == nil {
				op.writeErr = err
			}
			op.writesLeft--
			if op.writesLeft == 0 {
				op.commit()
			}
		})
		feed()
	}
}

// commit proposes the metadata switch to the new shards — the linearization
// point — then drops the old ones.
func (op *reencodeOp) commit() {
	if op.writeErr != nil {
		op.cleanupNew()
		op.finish(fmt.Errorf("coord: re-encode: writing new shards: %w", op.writeErr))
		return
	}
	op.c.cfg.Raft.Propose(meta.ReEncodeObject{
		ProposedAtUnixMS: op.atMS,
		Bucket:           op.bucket,
		Key:              op.key,
		VersionID:        op.entry.VersionID,
		DataID:           op.newDID,
		ECDataShards:     uint32(op.newK),
		ECParityShards:   uint32(op.newM),
		ShardChecksums:   op.newChecksums,
	}, func(_ any, err error) {
		if err != nil {
			op.cleanupNew()
			op.finish(fmt.Errorf("coord: re-encode: metadata commit: %w", err))
			return
		}
		// The version now names the new shards; the old ones are dead weight.
		// Best-effort delete — an orphan a scan reclaims, never a readable lie.
		for i := range op.oldNodes {
			op.c.cfg.Data.Delete(op.oldNodes[i], op.entry.DataID, uint32(i), func(error) {})
		}
		op.finish(nil)
	})
}

// cleanupNew drops the new shards after a pre-commit failure: the version still
// names the old ones, so the new are markerless garbage.
func (op *reencodeOp) cleanupNew() {
	for i := range op.newNodes {
		op.c.cfg.Data.Delete(op.newNodes[i], op.newDID, uint32(i), func(error) {})
	}
}

func (op *reencodeOp) finish(err error) {
	if op.finished {
		return
	}
	op.finished = true
	op.done(err)
}
