package meta

import (
	"errors"
	"fmt"
)

// ErrPersist marks a failure to commit a transaction to durable storage, as
// distinct from a deterministic validation error returned by apply. The
// distinction is load bearing on a Raft replica: a committed entry applies on
// every replica, so a local persist failure that silently reverted in-memory
// state (as apply does on any error) would diverge this replica from the
// peers that persisted. Callers applying committed entries treat it as fatal.
var ErrPersist = errors.New("meta: persist metadata transaction failed")

// Persistence: every apply is one transaction (METADATA.md principle 3),
// and the transaction is durable before it is visible. The store records
// each apply's row mutations; on success it hands the encoded changeset to
// the Persister in one atomic commit, and if that commit fails it rolls the
// in-memory state back — memory and disk can never diverge, in either
// direction. Reads stay in memory: metadata is small, hot, and local.

// Row is one keyspace row in its persisted form. A nil Value deletes the
// row (encoded records are never empty — format_version is always set).
type Row struct {
	Key   string
	Value []byte
}

// Persister carries one committed transaction's row changes to durable
// storage. Commit must be atomic — all rows or none — and the rows must be
// durable when it returns. internal/sys implements it over BadgerDB
// (ADR-0005); the simulation harness and tests substitute their own.
type Persister interface {
	Commit(rows []Row) error
}

// SetPersister attaches durable storage. Every subsequent apply commits its
// changeset through p before its effects become visible. Attach after
// restoring (Restore) and before serving traffic.
func (s *Store) SetPersister(p Persister) { s.persist = p }

// Restore loads one persisted row during startup, before the store serves
// anything. The owner replays every row the Persister holds; order does
// not matter.
func (s *Store) Restore(key string, value []byte) error {
	rec, err := decodeRow(key, value)
	if err != nil {
		return fmt.Errorf("restore metadata row %q: %w", key, err)
	}
	s.kv.memKV.set(key, rec)
	return nil
}

// Dump exports the store's entire state as encoded rows, sorted by key —
// deterministic: the same state dumps to the same bytes on every replica.
// This is the snapshot a Raft node ships to a lagging peer and writes when
// compacting its log; Restore is its inverse.
func (s *Store) Dump() []Row {
	var rows []Row
	s.kv.scan("", func(k string, v any) bool {
		rows = append(rows, Row{Key: k, Value: marshalRecord(v)})
		return true
	})
	return rows
}

// txn begins recording an apply's mutations and returns the function its
// deferred caller runs at exit: rollback on error, persist on success —
// and rollback again if persisting fails, surfacing the failure through
// errp. Apply methods use it as `defer s.txn(&err)()`.
func (s *Store) txn(errp *error) func() {
	s.kv.reset()
	return func() {
		t := s.kv
		if *errp != nil {
			t.rollback()
			return
		}
		if s.persist == nil || len(t.dirtyKeys) == 0 {
			return
		}
		rows := make([]Row, 0, len(t.dirtyKeys))
		for _, k := range t.dirtyKeys {
			row := Row{Key: k}
			if v, ok := t.memKV.get(k); ok {
				row.Value = marshalRecord(v)
			}
			rows = append(rows, row)
		}
		if err := s.persist.Commit(rows); err != nil {
			t.rollback()
			*errp = fmt.Errorf("%w: %w", ErrPersist, err)
		}
	}
}

// txKV wraps memKV, recording every mutation: the keys an apply touched
// (its changeset, first-touch order) and what stood there before (its undo
// log). Reads pass straight through.
type txKV struct {
	*memKV
	undo      []undoOp
	dirtyKeys []string
	dirty     map[string]bool
}

type undoOp struct {
	key     string
	prior   any
	existed bool
}

func newTxKV() *txKV {
	return &txKV{memKV: newMemKV(), dirty: make(map[string]bool)}
}

func (t *txKV) reset() {
	t.undo = t.undo[:0]
	t.dirtyKeys = t.dirtyKeys[:0]
	clear(t.dirty)
}

func (t *txKV) record(k string) {
	prior, existed := t.memKV.get(k)
	t.undo = append(t.undo, undoOp{key: k, prior: prior, existed: existed})
	if !t.dirty[k] {
		t.dirty[k] = true
		t.dirtyKeys = append(t.dirtyKeys, k)
	}
}

func (t *txKV) set(k string, v any) {
	t.record(k)
	t.memKV.set(k, v)
}

func (t *txKV) delete(k string) {
	t.record(k)
	t.memKV.delete(k)
}

// rollback restores the pre-transaction state, newest mutation first.
func (t *txKV) rollback() {
	for i := len(t.undo) - 1; i >= 0; i-- {
		op := t.undo[i]
		if op.existed {
			t.memKV.set(op.key, op.prior)
		} else {
			t.memKV.delete(op.key)
		}
	}
	t.undo = t.undo[:0]
}
