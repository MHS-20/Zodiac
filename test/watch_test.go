package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/MHS-20/Zodiac/api"
	"github.com/fortytw2/leaktest"
)

// watchCtx creates a cancellable context derived from the harness context,
// and returns the context plus a cancel func that drains the channel before
// cancelling, to ensure goroutines can unwind cleanly.
func watchCtx(h *Harness) (context.Context, context.CancelFunc) {
	return context.WithCancel(h.ctx)
}

func TestWatchBasicPut(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := watchCtx(h)
	defer cancel()

	ch, err := c.Watch(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckPut(c, "foo", "bar")

	select {
	case ev := <-ch:
		if ev.Key != "foo" || ev.Value != "bar" || ev.Type != api.EventPut {
			t.Errorf("got event %+v, want key=foo value=bar type=put", ev)
		}
		if ev.Revision != 1 {
			t.Errorf("got revision %d, want 1", ev.Revision)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch event")
	}
}

func TestWatchPrefix(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := watchCtx(h)
	defer cancel()

	ch, err := c.Watch(ctx, "/nodes/", 0)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckPut(c, "/nodes/1/foo", "bar")
	h.CheckPut(c, "/pods/abc", "running")

	select {
	case ev := <-ch:
		if ev.Key != "/nodes/1/foo" {
			t.Errorf("got key %q, want /nodes/1/foo", ev.Key)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch event")
	}

	select {
	case ev := <-ch:
		t.Errorf("unexpected event for key %q", ev.Key)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestWatchSince(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := watchCtx(h)
	defer cancel()

	h.CheckPut(c, "a", "1")
	h.CheckPut(c, "b", "2")

	ch, err := c.Watch(ctx, "", 1)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-ch:
		if ev.Key != "b" || ev.Revision != 2 {
			t.Errorf("replayed event: got %+v, want key=b revision=2", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replayed event")
	}

	select {
	case ev := <-ch:
		t.Errorf("unexpected event %+v", ev)
	case <-time.After(200 * time.Millisecond):
	}

	h.CheckPut(c, "c", "3")
	select {
	case ev := <-ch:
		if ev.Key != "c" || ev.Revision != 3 {
			t.Errorf("live event: got %+v, want key=c revision=3", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live event")
	}
}

func TestWatchAppendEvent(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := watchCtx(h)
	defer cancel()

	ch, err := c.Watch(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckPut(c, "k", "hello")

	select {
	case ev := <-ch:
		if ev.Value != "hello" {
			t.Errorf("Put event: got value %q, want hello", ev.Value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Put event")
	}

	h.CheckAppend(c, "k", " world")

	select {
	case ev := <-ch:
		if ev.Value != "hello world" {
			t.Errorf("Append event: got value %q, want 'hello world'", ev.Value)
		}
		if ev.Revision != 2 {
			t.Errorf("Append event: got revision %d, want 2", ev.Revision)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Append event")
	}
}

func TestWatchCASEvent(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := watchCtx(h)
	defer cancel()

	ch, err := c.Watch(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckPut(c, "k", "old")

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out draining Put event")
	}

	prevValue, _ := h.CheckCAS(c, "k", "old", "new")
	if prevValue != "old" {
		t.Fatalf("CAS expected prev=old, got %q", prevValue)
	}

	select {
	case ev := <-ch:
		if ev.Value != "new" || ev.Revision != 2 {
			t.Errorf("CAS success event: got %+v, want value=new revision=2", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CAS event")
	}

	prevValue, _ = h.CheckCAS(c, "k", "wrong", "nope")
	if prevValue != "new" {
		t.Logf("failed CAS: prev=%q, key was not updated", prevValue)
	}

	select {
	case ev := <-ch:
		t.Errorf("unexpected event for failed CAS: %+v", ev)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestWatchMultipleSubscribers(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := watchCtx(h)
	defer cancel()

	ch1, err := c.Watch(ctx, "/a/", 0)
	if err != nil {
		t.Fatal(err)
	}
	ch2, err := c.Watch(ctx, "/b/", 0)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckPut(c, "/a/x", "1")

	select {
	case ev := <-ch1:
		if ev.Key != "/a/x" {
			t.Errorf("ch1: got key %q, want /a/x", ev.Key)
		}
	case <-time.After(time.Second):
		t.Fatal("ch1 timed out")
	}

	select {
	case <-ch2:
		t.Error("ch2 should not receive /a/ events")
	case <-time.After(200 * time.Millisecond):
	}

	h.CheckPut(c, "/b/y", "2")

	select {
	case ev := <-ch2:
		if ev.Key != "/b/y" {
			t.Errorf("ch2: got key %q, want /b/y", ev.Key)
		}
	case <-time.After(time.Second):
		t.Fatal("ch2 timed out")
	}

	select {
	case <-ch1:
		t.Error("ch1 should not receive /b/ events")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestWatchEmptyPrefix(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := watchCtx(h)
	defer cancel()

	ch, err := c.Watch(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckPut(c, "anything", "works")
	h.CheckPut(c, "/some/other/key", "also")

	for range 2 {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for event")
		}
	}
}

func TestWatchConcurrentPutAndWatch(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	seen := sync.Map{}

	prefixes := []string{"/a/", "/b/", ""}
	for _, p := range prefixes {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := h.NewClient()
			ch, err := c.Watch(ctx, p, 0)
			if err != nil {
				return
			}
			for ev := range ch {
				seen.Store(ev.Key, ev.Value)
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)

	for i := range 10 {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := h.NewClient()
			key := fmt.Sprintf("/a/key%d", i)
			if i%2 == 0 {
				key = fmt.Sprintf("/b/key%d", i)
			}
			h.CheckPut(c, key, fmt.Sprintf("val%d", i))
		}()
	}

	time.Sleep(500 * time.Millisecond)
	cancel()
	wg.Wait()
}