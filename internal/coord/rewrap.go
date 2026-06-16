package coord

import (
	"errors"
	"fmt"

	"github.com/hamster-storage/hamster/internal/keys"
	"github.com/hamster-storage/hamster/internal/meta"
)

// The master-key rotation sweep (ADR-0032): a metadata-only rewrap. When a
// rotation is open (the posture carries a rotating-to fingerprint), the leader
// walks every encrypted version still wrapped under the old key, unwraps its
// DEK under the old KEK and rewraps it under the new one, and commits the new
// wrapped DEK through a RewrapDEK proposal. Object bytes, shards, and shard
// checksums are never touched — only the ~60-byte wrapped key changes — so the
// sweep stays off the data path and is COMPLIANCE-safe by construction (a
// locked version is rewrapped with its lock and bytes intact).
//
// The sweep is leader-only and runs under the shared single-flight guard, like
// repair and the scrubber, so a rotation never interleaves with a layout
// migration. It is sequential: one RewrapDEK at a time, each awaited, so by the
// time the pass ends every rewrap is applied and a final re-scan sees the true
// straggler count. A pass that leaves no version on the old key closes the
// rotation (CompleteKEKRotation), advancing the cluster onto the new key so the
// old one can be retired; a pass that could not rewrap everything (a failed
// commit) leaves the rotation open and reports it, and the next sweep retries.

// RewrapReport is one rewrap sweep's outcome.
type RewrapReport struct {
	// Objects is how many encrypted versions the sweep examined.
	Objects int
	// Rewrapped is how many were rewrapped old→new this pass.
	Rewrapped int
	// AlreadyNew is how many were already on the new key (a resumed sweep).
	AlreadyNew int
	// Remaining is how many versions are still on the old key after the pass —
	// the rotation's provable progress. Zero means it converged and the
	// rotation was closed.
	Remaining int
	// Completed reports whether this sweep closed the rotation.
	Completed bool
	// Failed lists versions whose rewrap could not be committed; the next
	// sweep retries them.
	Failed []string
}

// ErrNoRotation is returned when RewrapSweep runs with no rotation open — the
// posture carries no rotating-to fingerprint. Not an error the caller must act
// on: there is simply nothing to rewrap.
var ErrNoRotation = errors.New("coord: no master-key rotation is open")

// RewrapSweep runs one rewrap pass for an open master-key rotation (ADR-0032).
// done fires exactly once on the loop. Only one sweep may run at a time per
// coordinator; leader-only (the caller gates on leadership), since it proposes.
func (c *Coordinator) RewrapSweep(done func(RewrapReport, error)) {
	if !c.beginSweep() {
		done(RewrapReport{}, ErrSweepBusy)
		return
	}
	end := func(r RewrapReport, e error) { c.endSweep(); done(r, e) }

	post := c.cfg.Raft.Store().EncryptionPosture()
	if post.Algorithm == meta.EncNone || post.RotatingToKEKFingerprint == 0 {
		end(RewrapReport{}, ErrNoRotation)
		return
	}
	oldFP, newFP := post.CurrentKEKFingerprint, post.RotatingToKEKFingerprint
	oldKEK, ok := c.keyFor(oldFP)
	if !ok || !oldKEK.Loaded() {
		end(RewrapReport{}, fmt.Errorf("coord: rewrap needs the old KEK %016x loaded", oldFP))
		return
	}
	newKEK, ok := c.keyFor(newFP)
	if !ok || !newKEK.Loaded() {
		end(RewrapReport{}, fmt.Errorf("coord: rewrap needs the new KEK %016x loaded", newFP))
		return
	}

	op := &rewrapOp{
		c: c, oldKEK: oldKEK, newKEK: newKEK, oldFP: oldFP, newFP: newFP,
		work: c.collectEncrypted(),
		done: end,
	}
	op.next()
}

// collectEncrypted snapshots every encrypted whole-object version the metadata
// names — the rewrap work list. Delete markers and multipart entries carry no
// single wrapped DEK and are skipped.
func (c *Coordinator) collectEncrypted() []sweepItem {
	var work []sweepItem
	store := c.cfg.Raft.Store()
	for _, b := range store.ListBuckets() {
		bucket := b.Name
		store.ScanVersions(bucket, func(key string, e meta.VersionEntry) bool {
			if e.Kind == meta.KindObject && len(e.Parts) == 0 && e.EncAlgorithm != meta.EncNone {
				work = append(work, sweepItem{bucket: bucket, key: key, entry: e})
			}
			return true
		})
	}
	return work
}

type rewrapOp struct {
	c              *Coordinator
	oldKEK, newKEK keys.KEK
	oldFP, newFP   uint64
	work           []sweepItem
	report         RewrapReport
	done           func(RewrapReport, error)
}

func (op *rewrapOp) next() {
	if len(op.work) == 0 {
		op.finish()
		return
	}
	item := op.work[0]
	op.work = op.work[1:]
	op.report.Objects++
	e := item.entry

	// Already on the new key — a resumed sweep, or a write that landed on the
	// new key during the rotation. Nothing to do.
	if e.KEKFingerprint == op.newFP {
		op.report.AlreadyNew++
		op.next()
		return
	}

	// A straggler: its DEK is wrapped under the old key (the founding key reads
	// as fingerprint 0, which the rotation established as the current key). Any
	// other fingerprint is not part of this rotation — leave it untouched.
	if e.KEKFingerprint != op.oldFP && e.KEKFingerprint != 0 {
		op.next()
		return
	}

	dek, err := op.oldKEK.Unwrap(e.WrappedDEK)
	if err != nil {
		op.fail(item, fmt.Errorf("unwrap under old KEK: %w", err))
		return
	}
	// Rewrap under the new KEK. The wrap nonce is the version's data ID — the
	// same globally-unique value the original wrap used, which is fresh under
	// the new KEK, so no (KEK, nonce) pair ever repeats.
	wrapped, err := op.newKEK.Wrap(dek, wrapNonce(e.DataID))
	if err != nil {
		op.fail(item, fmt.Errorf("rewrap under new KEK: %w", err))
		return
	}
	op.c.cfg.Raft.Propose(meta.RewrapDEK{
		ProposedAtUnixMS: op.c.cfg.Clock.Now().UnixMilli(),
		Bucket:           item.bucket,
		Key:              item.key,
		VersionID:        e.VersionID,
		WrappedDEK:       wrapped,
		KEKFingerprint:   op.newFP,
	}, func(_ any, err error) {
		if err != nil {
			op.report.Failed = append(op.report.Failed, fmt.Sprintf("%s/%s: commit rewrap: %v", item.bucket, item.key, err))
		} else {
			op.report.Rewrapped++
		}
		// Defer off the Raft apply callback: proposing the next item — or the
		// completion in finish — synchronously here would re-enter the raft node
		// before it advances its current Ready. AfterFunc runs next on the
		// following loop turn, after Advance.
		op.c.cfg.Clock.AfterFunc(0, op.next)
	})
}

// fail records a pre-propose error (unwrap or rewrap) and advances. Reached
// from next on the loop, never from inside a Raft apply callback, so it may
// continue synchronously.
func (op *rewrapOp) fail(item sweepItem, err error) {
	op.report.Failed = append(op.report.Failed, fmt.Sprintf("%s/%s: %v", item.bucket, item.key, err))
	op.next()
}

// finish closes the pass: re-scan for any version still on the old key (the
// proposals are all applied by now, the sweep being sequential), and close the
// rotation when none remain. A re-scan rather than a counter so a write that
// landed on the old key after the work snapshot is still seen — the rotation
// closes only when the keyspace truly holds no straggler.
func (op *rewrapOp) finish() {
	remaining := 0
	for _, item := range op.c.collectEncrypted() {
		if fp := item.entry.KEKFingerprint; fp != op.newFP {
			remaining++
		}
	}
	op.report.Remaining = remaining
	if remaining > 0 {
		op.done(op.report, nil) // not converged; the caller re-runs
		return
	}
	op.c.cfg.Raft.Propose(meta.CompleteKEKRotation{
		ProposedAtUnixMS: op.c.cfg.Clock.Now().UnixMilli(),
		ToFingerprint:    op.newFP,
	}, func(_ any, err error) {
		if err != nil {
			op.report.Failed = append(op.report.Failed, fmt.Sprintf("complete rotation: %v", err))
			op.done(op.report, nil)
			return
		}
		op.report.Completed = true
		op.done(op.report, nil)
	})
}
