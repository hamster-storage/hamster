package sim

import (
	"slices"
	"testing"
	"time"
)

func TestLoopPostOrdering(t *testing.T) {
	s, w := crashWorld(9)
	loop := (*w).Loop
	clock := (*w).Clock
	var got []string
	// Two timers at the same virtual instant: they fire in scheduling
	// order, and work posted by the first runs after events already queued
	// for that instant.
	clock.AfterFunc(time.Second, func() {
		got = append(got, "timer")
		loop.Post(func() { got = append(got, "post-1") })
		loop.Post(func() { got = append(got, "post-2") })
	})
	clock.AfterFunc(time.Second, func() { got = append(got, "peer") })
	s.Run(time.Minute)

	want := []string{"timer", "peer", "post-1", "post-2"}
	if !slices.Equal(got, want) {
		t.Fatalf("events ran as %v, want %v", got, want)
	}
}

func TestCrashDropsPostedWork(t *testing.T) {
	s, w := crashWorld(10)
	ran := false
	(*w).Loop.Post(func() { ran = true })
	s.Crash("n1")
	s.Restart("n1")
	s.Run(time.Minute)
	if ran {
		t.Fatal("work posted before a crash ran after restart")
	}
}
