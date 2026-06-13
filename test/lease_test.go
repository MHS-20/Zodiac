package test

import (
	"context"
	"testing"
	"time"

	"github.com/MHS-20/Zodiac/api"
	"github.com/fortytw2/leaktest"
)

func TestLeaseGrant(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	id, ttl, err := c.LeaseGrant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Errorf("expected lease ID 1, got %d", id)
	}
	if ttl != 60 {
		t.Errorf("expected TTL 60, got %d", ttl)
	}
}

func TestLeaseKeepAlive(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	_, _, err := c.LeaseGrant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}

	err = c.LeaseKeepAlive(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLeaseRevoke(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	_, _, err := c.LeaseGrant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckPutWithLease(c, "k", "v", 1)
	h.CheckGet(c, "k", "v")

	err = c.LeaseRevoke(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckGetNotFound(c, "k")
}

func TestLeasePutWithLeaseID(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	_, _, err := c.LeaseGrant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckPutWithLease(c, "/leased/key", "val", 1)
	h.CheckGet(c, "/leased/key", "val")

	err = c.LeaseRevoke(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckGetNotFound(c, "/leased/key")
}

func TestLeaseExpiry(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
	defer cancel()

	_, _, err := c.LeaseGrant(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckPutWithLease(c, "expkey", "x", 1)
	h.CheckGet(c, "expkey", "x")

	time.Sleep(2500 * time.Millisecond)

	h.CheckGetNotFound(c, "expkey")
}

func TestLeaseRevokeWatchEvent(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()

	ch, err := c.Watch(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckPut(c, "start", "x")
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out draining start event")
	}

	_, _, err = c.LeaseGrant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckPutWithLease(c, "leasekey", "v", 1)
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for put event")
	}

	err = c.LeaseRevoke(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-ch:
		if ev.Key != "leasekey" || ev.Type != api.EventDelete {
			t.Errorf("got event %+v, want key=leasekey type=delete", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delete event from revoke")
	}
}

func TestLeaseDuplicateGrant(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	id1, _, err := c.LeaseGrant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != 1 {
		t.Errorf("expected id 1, got %d", id1)
	}

	id2, _, err := c.LeaseGrant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if id2 != 2 {
		t.Errorf("expected id 2, got %d", id2)
	}
}

func TestLeaseMultipleKeys(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	_, _, err := c.LeaseGrant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckPutWithLease(c, "key1", "a", 1)
	h.CheckPutWithLease(c, "key2", "b", 1)
	h.CheckPutWithLease(c, "key3", "c", 1)

	h.CheckGet(c, "key1", "a")
	h.CheckGet(c, "key2", "b")
	h.CheckGet(c, "key3", "c")

	err = c.LeaseRevoke(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckGetNotFound(c, "key1")
	h.CheckGetNotFound(c, "key2")
	h.CheckGetNotFound(c, "key3")
}

func TestLeaseTxnWithLeaseID(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	_, _, err := c.LeaseGrant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}

	success := []api.TxnOp{
		{Op: api.TxnOpPut, Key: "txnkey", Value: "val", LeaseID: 1},
	}
	_, _, _, err = c.Txn(ctx, nil, success, nil)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckGet(c, "txnkey", "val")

	err = c.LeaseRevoke(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckGetNotFound(c, "txnkey")
}

func TestLeaseExpiryWatchEvent(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()

	ch, err := c.Watch(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckPut(c, "start", "x")
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out draining start event")
	}

	_, _, err = c.LeaseGrant(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	h.CheckPutWithLease(c, "expwatch", "val", 1)
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for put event")
	}

	time.Sleep(2500 * time.Millisecond)

	select {
	case ev := <-ch:
		if ev.Key != "expwatch" || ev.Type != api.EventDelete {
			t.Errorf("got event %+v, want key=expwatch type=delete", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delete event from expiry")
	}
}
