package test

import (
	"context"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func TestListPaginationBasic(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	for i := range 10 {
		key := string(rune('a' + i))
		h.CheckPut(c, key, "v")
	}

	pairs, nextKey, _, err := c.ListPaged(ctx, "", 3, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 3 {
		t.Errorf("expected 3 pairs, got %d", len(pairs))
	}
	if nextKey != "d" {
		t.Errorf("expected nextKey=d, got %q", nextKey)
	}
	if pairs["a"] != "v" || pairs["b"] != "v" || pairs["c"] != "v" {
		t.Errorf("unexpected pairs: %v", pairs)
	}
}

func TestListPaginationFullIteration(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	for i := range 7 {
		key := string(rune('a' + i))
		h.CheckPut(c, key, "v")
	}

	var allKeys []string
	nextKey := ""
	limit := 3

	for {
		pairs, nk, _, err := c.ListPaged(ctx, "", limit, nextKey)
		if err != nil {
			t.Fatal(err)
		}
		for k := range pairs {
			allKeys = append(allKeys, k)
		}
		nextKey = nk
		if nextKey == "" {
			break
		}
	}

	if len(allKeys) != 7 {
		t.Errorf("expected 7 keys total, got %d: %v", len(allKeys), allKeys)
	}
}

func TestListPaginationLimitLargerThanAvailable(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	h.CheckPut(c, "a", "1")
	h.CheckPut(c, "b", "2")

	pairs, nextKey, _, err := c.ListPaged(ctx, "", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 2 {
		t.Errorf("expected 2 pairs, got %d", len(pairs))
	}
	if nextKey != "" {
		t.Errorf("expected empty nextKey, got %q", nextKey)
	}
}

func TestListPaginationWithPrefix(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	h.CheckPut(c, "/a/1", "x")
	h.CheckPut(c, "/a/2", "y")
	h.CheckPut(c, "/a/3", "z")
	h.CheckPut(c, "/b/1", "w")

	pairs, nextKey, _, err := c.ListPaged(ctx, "/a/", 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 2 {
		t.Errorf("expected 2 pairs, got %d", len(pairs))
	}
	if nextKey != "/a/3" {
		t.Errorf("expected nextKey=/a/3, got %q", nextKey)
	}

	pairs, nextKey, _, err = c.ListPaged(ctx, "/a/", 2, nextKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 1 {
		t.Errorf("expected 1 pair, got %d", len(pairs))
	}
	if nextKey != "" {
		t.Errorf("expected empty nextKey, got %q", nextKey)
	}
	if pairs["/a/3"] != "z" {
		t.Errorf("expected /a/3=z, got %v", pairs)
	}
}

func TestListPaginationEmptyResult(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	pairs, nextKey, _, err := c.ListPaged(ctx, "/nonexistent/", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 0 {
		t.Errorf("expected 0 pairs, got %d", len(pairs))
	}
	if nextKey != "" {
		t.Errorf("expected empty nextKey, got %q", nextKey)
	}
}
