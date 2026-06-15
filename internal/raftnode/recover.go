package raftnode

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/wal"
)

// RecoverySummary reports what ForceNewCluster did, for the operator.
type RecoverySummary struct {
	// LastIndex is the log position the new cluster starts from: every
	// entry in the survivor's log up to here is now committed history.
	LastIndex uint64
	// Removed lists the ex-members dropped from the configuration. Their
	// data directories must never run again: they hold a competing
	// history.
	Removed []Member
}

// ForceNewCluster rewrites a stopped node's on-disk Raft state into a new
// single-voter cluster — the disaster exit (ADR-0025) for a cluster whose
// quorum is permanently lost. The survivor's local log wins, all of it:
// entries past the old commit point may never have been acknowledged
// anywhere, but the dead majority may also have committed them — local
// truth is the only truth left, and dropping it could lose acknowledged
// writes. The result is one rotation frame: a snapshot at the last local
// index whose configuration is this node, alone, as voter. Idempotent — a
// crash mid-recovery falls back to the previous state, and running it
// again finishes the job.
func ForceNewCluster(disk seam.Disk, id uint64, node seam.NodeID, dial string) (RecoverySummary, error) {
	names, err := disk.List()
	if err != nil {
		return RecoverySummary{}, fmt.Errorf("raftnode: listing disk: %w", err)
	}
	var seqs []uint64
	for _, name := range names {
		if rest, ok := strings.CutPrefix(name, "raft/log."); ok {
			seq, err := strconv.ParseUint(rest, 10, 64)
			if err != nil {
				return RecoverySummary{}, fmt.Errorf("raftnode: alien log file %q", name)
			}
			seqs = append(seqs, seq)
		}
	}
	if len(seqs) == 0 {
		return RecoverySummary{}, fmt.Errorf("raftnode: no raft state to recover from")
	}
	slices.Sort(seqs)

	// Load the newest valid log exactly the way boot does.
	var records [][]byte
	loaded := false
	for i := len(seqs) - 1; i >= 0; i-- {
		_, recs, err := wal.Open(disk, logName(seqs[i]))
		if err != nil {
			return RecoverySummary{}, err
		}
		if !validLog(recs, i == 0) {
			continue
		}
		records, loaded = recs, true
		break
	}
	if !loaded {
		return RecoverySummary{}, fmt.Errorf("raftnode: no valid log file among %d candidates", len(seqs))
	}

	storage := raft.NewMemoryStorage()
	store := meta.NewStore()
	peers := make(map[uint64]peerInfo)
	removed := make(map[uint64]struct{})
	for i, raw := range records {
		rec, err := decodeRecord(raw)
		if err != nil {
			return RecoverySummary{}, fmt.Errorf("raftnode: record %d: %w", i, err)
		}
		if !raft.IsEmptySnap(rec.snap) {
			snapStore, members, snapRemoved, err := decodeSnapshotData(rec.snap.Data)
			if err != nil {
				return RecoverySummary{}, fmt.Errorf("raftnode: record %d snapshot: %w", i, err)
			}
			if err := storage.ApplySnapshot(rec.snap); err != nil {
				return RecoverySummary{}, fmt.Errorf("raftnode: record %d snapshot: %w", i, err)
			}
			store = snapStore
			peers = members
			removed = snapRemoved
		}
		if err := storage.Append(rec.entries); err != nil {
			return RecoverySummary{}, fmt.Errorf("raftnode: record %d: %w", i, err)
		}
		if !raft.IsEmptyHardState(rec.hs) {
			if err := storage.SetHardState(rec.hs); err != nil {
				return RecoverySummary{}, fmt.Errorf("raftnode: record %d: %w", i, err)
			}
		}
	}

	// Apply the whole log — committed prefix and tail alike — to the
	// store and the address book.
	hs, _, err := storage.InitialState()
	if err != nil {
		return RecoverySummary{}, err
	}
	first, err := storage.FirstIndex()
	if err != nil {
		return RecoverySummary{}, err
	}
	last, err := storage.LastIndex()
	if err != nil {
		return RecoverySummary{}, err
	}
	if last >= first {
		ents, err := storage.Entries(first, last+1, math.MaxUint64)
		if err != nil {
			return RecoverySummary{}, err
		}
		for _, e := range ents {
			switch {
			case e.Type == raftpb.EntryNormal && len(e.Data) > 0:
				p, err := meta.DecodeProposal(e.Data)
				if err != nil {
					return RecoverySummary{}, fmt.Errorf("raftnode: entry %d: %w", e.Index, err)
				}
				_, _ = store.Apply(p) // deterministic refusals are outcomes, not failures
			case e.Type == raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				if err := cc.Unmarshal(e.Data); err != nil {
					return RecoverySummary{}, fmt.Errorf("raftnode: conf change at %d: %w", e.Index, err)
				}
				switch cc.Type {
				case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
					if len(cc.Context) > 0 {
						if mid, info, err := decodeMember(cc.Context); err == nil && mid == cc.NodeID {
							peers[mid] = info
						}
					}
				case raftpb.ConfChangeRemoveNode:
					delete(peers, cc.NodeID)
					removed[cc.NodeID] = struct{}{} // preserve the tombstone across recovery
				}
			}
		}
	}
	lastTerm, err := storage.Term(last)
	if err != nil {
		return RecoverySummary{}, err
	}

	var summary RecoverySummary
	summary.LastIndex = last
	for _, mid := range slices.Sorted(maps.Keys(peers)) {
		if mid != id {
			summary.Removed = append(summary.Removed, Member{ID: mid, Addr: peers[mid].node, Dial: peers[mid].dial})
		}
	}

	// The new history: one frame, snapshot at the last index, this node
	// the only voter. Durable before the old files go.
	self := peerInfo{node: node, dial: dial}
	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: last, Term: lastTerm,
			ConfState: raftpb.ConfState{Voters: []uint64{id}},
		},
		Data: encodeSnapshotData(store.Dump(), map[uint64]peerInfo{id: self}, removed),
	}
	newHS := raftpb.HardState{Term: hs.Term, Vote: hs.Vote, Commit: last}

	newSeq := seqs[len(seqs)-1] + 1
	log, leftover, err := wal.Open(disk, logName(newSeq))
	if err != nil || len(leftover) != 0 {
		return RecoverySummary{}, fmt.Errorf("raftnode: opening recovery log: %d leftover records, %v", len(leftover), err)
	}
	if err := log.Append(encodeRecord(record{hs: newHS, snap: snap})); err != nil {
		return RecoverySummary{}, fmt.Errorf("raftnode: writing recovery log: %w", err)
	}
	for _, seq := range seqs {
		if err := disk.Remove(logName(seq)); err != nil {
			return RecoverySummary{}, fmt.Errorf("raftnode: removing old log %d: %w", seq, err)
		}
		_ = disk.Sync(logName(seq))
	}
	return summary, nil
}
