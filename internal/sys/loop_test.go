package sys

import (
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestLoopSerializesPosts(t *testing.T) {
	l := NewLoop()
	defer l.Stop()
	counter := 0 // unsynchronized on purpose: the loop is the synchronization
	const goroutines, posts = 8, 1000
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range posts {
				l.Post(func() { counter++ })
			}
		}()
	}
	wg.Wait()
	done := make(chan struct{})
	l.Post(func() { close(done) }) // all prior posts are queued ahead of this
	<-done
	if counter != goroutines*posts {
		t.Fatalf("counter = %d, want %d — posts were not serialized", counter, goroutines*posts)
	}
}

func TestLoopFIFOAndSelfPost(t *testing.T) {
	l := NewLoop()
	defer l.Stop()
	var got []int
	done := make(chan struct{})
	for i := range 100 {
		l.Post(func() { got = append(got, i) })
	}
	l.Post(func() {
		// A loop function may post to its own loop; with a bounded queue
		// this could deadlock, which is why the queue is unbounded.
		l.Post(func() {
			got = append(got, 100)
			close(done)
		})
	})
	<-done
	if len(got) != 101 {
		t.Fatalf("ran %d functions, want 101", len(got))
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("got[%d] = %d; posts ran out of order", i, v)
		}
	}
}

func TestLoopStop(t *testing.T) {
	l := NewLoop()
	l.Stop()
	l.Stop() // stopping twice is fine
	l.Post(func() { t.Error("a post after Stop ran") })
}

func TestLoopClockDeliversOnLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := NewLoop()
		defer l.Stop()
		clock := LoopClock(Clock{}, l)
		var got []string // owned by the loop; the race detector enforces it
		done := make(chan struct{})
		clock.AfterFunc(time.Hour, func() {
			got = append(got, "timer")
			close(done)
		})
		l.Post(func() { got = append(got, "post") })
		<-done
		if want := []string{"post", "timer"}; !slices.Equal(got, want) {
			t.Fatalf("loop ran %v, want %v", got, want)
		}
	})
}

func TestLoopClockStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := NewLoop()
		defer l.Stop()
		clock := LoopClock(Clock{}, l)
		timer := clock.AfterFunc(time.Hour, func() { t.Error("stopped timer fired") })
		if !timer.Stop() {
			t.Fatal("Stop on a pending timer reported false")
		}
		if timer.Stop() {
			t.Fatal("second Stop reported true")
		}
	})
}
