// Package seam defines the interfaces between Hamster's core logic and the
// outside world: the event loop, time, the network, and the disk.
//
// Core code never touches the OS directly; it receives these interfaces.
// Each one has two implementations: a deterministic, fault-injectable one
// provided by the simulator (internal/sim) and a thin OS-backed one for
// production (internal/sys). The production adapters contain no decisions —
// any code that chooses lives on the core side of this seam — so the same
// compiled core runs under both, and what the simulator proves is what
// ships. See docs/SIMULATION.md.
//
// Randomness needs no interface: core code receives a *math/rand/v2.Rand,
// which is deterministic by construction once the simulator picks its seed.
package seam

import "time"

// NodeID identifies a node in the cluster.
type NodeID string

// Loop is a node's event loop: the single logical thread that owns all of
// the node's core state (docs/SIMULATION.md — "core state is owned by its
// event loop, full stop").
//
// Post delivers fn to the loop. Functions run one at a time, each to
// completion, and posts from a single caller run in post order. Post never
// blocks and is safe to call from any goroutine: it is how the data plane
// and the adapters hand results back to the core ("shard 3 durable"), and
// how core code defers work to itself.
//
// Posted work dies with the process, like timers: a function posted before
// a crash (simulation) or stop (production) may never run.
type Loop interface {
	Post(fn func())
}

// Clock provides the current time and timer scheduling.
//
// Timer callbacks are delivered on the node's event loop, never on a
// separate thread: in simulation they are events in the global queue, and in
// production the node runtime hands them to its loop. Core code may
// therefore mutate node state inside a callback without locking.
type Clock interface {
	// Now returns the current time. Under simulation this is virtual time,
	// and different nodes may disagree (clock skew is part of the fault
	// model) — never compare timestamps across nodes for ordering.
	Now() time.Time

	// AfterFunc schedules fn to run once, after at least d has elapsed.
	AfterFunc(d time.Duration, fn func()) Timer
}

// Timer is a handle to a pending AfterFunc callback.
type Timer interface {
	// Stop cancels the timer. It reports false if the timer already fired
	// (its callback ran, or is committed to run) or was already stopped.
	Stop() bool
}

// Transport sends messages to other nodes.
//
// Delivery is unreliable and unordered: a message may be delayed, reordered,
// duplicated, or dropped, and the receiver may have restarted since the
// send. Anything stronger is the core's job to build on top. Received
// messages arrive on the node's event loop via its message handler, never on
// a separate thread.
type Transport interface {
	// Send queues msg for delivery to the named node and returns
	// immediately. Send never blocks and never reports delivery. The caller
	// may reuse msg after Send returns.
	Send(to NodeID, msg []byte)
}

// MessageHandler is the receiving half of the network contract — Transport
// sends, a node's core logic receives. Core logic implements it; the drivers
// on the other side of the seam consume it: the simulator calls HandleMessage
// when it delivers a simulated message, and the production listener calls it
// when bytes arrive on a real socket.
//
// Messages arrive one at a time on the node's event loop, never on a
// separate thread, so implementations need no locking.
type MessageHandler interface {
	// HandleMessage delivers one message from the network.
	HandleMessage(from NodeID, msg []byte)
}

// Disk is a node's durable storage: a namespace of files addressed by
// slash-separated relative paths (names must satisfy io/fs.ValidPath).
//
// Writes are staged, not durable: data from WriteFile or Append survives a
// crash only after a successful Sync of the same name. After a crash, an
// unsynced file may hold its previous content, a prefix of the new data (a
// torn write), or the complete new data. Removes are staged the same way.
//
// Files are written once and never edited in place, because objects are
// immutable blobs (see CLAUDE.md). Append exists for the write buffer —
// building a file incrementally with bounded memory before its one Sync —
// not for mutating synced files.
type Disk interface {
	// WriteFile stages data under name, replacing any staged or durable
	// content. The caller may reuse data after WriteFile returns.
	WriteFile(name string, data []byte) error

	// Append stages data added to the end of name's current content —
	// staged content if any, else durable content, else a new empty file.
	// Like WriteFile it is durable only after Sync; after a crash, content
	// that was already durable before the appends survives, and the
	// appended bytes may be lost entirely or land as a torn prefix. The
	// caller may reuse data after Append returns.
	Append(name string, data []byte) error

	// Sync makes all staged changes to name (writes or a remove) durable.
	// Syncing a name with no staged changes is a no-op.
	Sync(name string) error

	// ReadFile returns the current content of name, staged or durable. It
	// returns an error satisfying errors.Is(err, fs.ErrNotExist) if the
	// file does not exist.
	ReadFile(name string) ([]byte, error)

	// Remove stages deletion of name. The deletion is durable only after
	// Sync(name). Removing a file that does not exist returns an error
	// satisfying errors.Is(err, fs.ErrNotExist).
	Remove(name string) error

	// List returns the names of all current files, sorted.
	List() ([]string, error)
}
