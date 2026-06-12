package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/MHS-20/Zodiac/api"
	"github.com/fortytw2/leaktest"
)

func TestTxnBasicSuccess(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	h.CheckPut(c, "k", "old")

	conds := []api.TxnCondition{
		{Key: "k", Compare: api.CompareEQ, Value: "old"},
	}
	success := []api.TxnOp{
		{Op: api.TxnOpPut, Key: "k", Value: "new"},
	}
	failure := []api.TxnOp{
		{Op: api.TxnOpPut, Key: "k", Value: "failed"},
	}
	succeeded, results, rev, err := c.Txn(ctx, conds, success, failure)
	if err != nil {
		t.Fatal(err)
	}
	if !succeeded {
		t.Error("expected txn to succeed")
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Key != "k" || results[0].PrevValue != "old" || !results[0].KeyFound {
		t.Errorf("result: got %+v, want key=k prev=old found=true", results[0])
	}
	if rev != 2 {
		t.Errorf("expected revision 2, got %d", rev)
	}
	h.CheckGet(c, "k", "new")
}

func TestTxnBasicFailure(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	h.CheckPut(c, "k", "old")

	conds := []api.TxnCondition{
		{Key: "k", Compare: api.CompareEQ, Value: "wrong"},
	}
	success := []api.TxnOp{
		{Op: api.TxnOpPut, Key: "k", Value: "success"},
	}
	failure := []api.TxnOp{
		{Op: api.TxnOpPut, Key: "k", Value: "failure"},
	}
	succeeded, results, rev, err := c.Txn(ctx, conds, success, failure)
	if err != nil {
		t.Fatal(err)
	}
	if succeeded {
		t.Error("expected txn to fail")
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Key != "k" || results[0].PrevValue != "old" || !results[0].KeyFound {
		t.Errorf("result: got %+v, want key=k prev=old found=true", results[0])
	}
	if rev != 2 {
		t.Errorf("expected revision 2, got %d", rev)
	}
	h.CheckGet(c, "k", "failure")
}

func TestTxnNoConditions(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	success := []api.TxnOp{
		{Op: api.TxnOpPut, Key: "k", Value: "v"},
	}
	succeeded, results, rev, err := c.Txn(ctx, nil, success, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !succeeded {
		t.Error("expected txn with no conditions to succeed")
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if rev != 1 {
		t.Errorf("expected revision 1, got %d", rev)
	}
	h.CheckGet(c, "k", "v")
}

func TestTxnMultiOp(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	success := []api.TxnOp{
		{Op: api.TxnOpPut, Key: "/a/1", Value: "x"},
		{Op: api.TxnOpPut, Key: "/a/2", Value: "y"},
	}
	succeeded, results, rev, err := c.Txn(ctx, nil, success, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !succeeded {
		t.Error("expected txn to succeed")
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if rev != 1 {
		t.Errorf("expected revision 1, got %d", rev)
	}
	h.CheckGet(c, "/a/1", "x")
	h.CheckGet(c, "/a/2", "y")
}

func TestTxnCAS(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	h.CheckPut(c, "k", "10")

	conds := []api.TxnCondition{
		{Key: "k", Compare: api.CompareGTE, Value: "5"},
	}
	success := []api.TxnOp{
		{Op: api.TxnOpCAS, Key: "k", CompareValue: "10", Value: "20"},
	}
	succeeded, results, _, err := c.Txn(ctx, conds, success, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !succeeded {
		t.Error("expected txn to succeed")
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].PrevValue != "10" || !results[0].KeyFound {
		t.Errorf("CAS result: got %+v, want prev=10 found=true", results[0])
	}
	h.CheckGet(c, "k", "20")
}

func TestTxnAppend(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	h.CheckPut(c, "k", "hello")

	conds := []api.TxnCondition{
		{Key: "k", Compare: api.CompareExists},
	}
	success := []api.TxnOp{
		{Op: api.TxnOpAppend, Key: "k", Value: " world"},
	}
	failure := []api.TxnOp{
		{Op: api.TxnOpPut, Key: "k", Value: "not found"},
	}
	succeeded, results, _, err := c.Txn(ctx, conds, success, failure)
	if err != nil {
		t.Fatal(err)
	}
	if !succeeded {
		t.Error("expected txn to succeed")
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].PrevValue != "hello" || !results[0].KeyFound {
		t.Errorf("Append result: got %+v, want prev=hello found=true", results[0])
	}
	h.CheckGet(c, "k", "hello world")
}

func TestTxnDelete(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	h.CheckPut(c, "k", "gone")

	conds := []api.TxnCondition{
		{Key: "k", Compare: api.CompareExists},
	}
	success := []api.TxnOp{
		{Op: api.TxnOpDelete, Key: "k"},
	}
	failure := []api.TxnOp{
		{Op: api.TxnOpPut, Key: "k", Value: "nope"},
	}
	succeeded, results, _, err := c.Txn(ctx, conds, success, failure)
	if err != nil {
		t.Fatal(err)
	}
	if !succeeded {
		t.Error("expected txn to succeed")
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].PrevValue != "gone" || !results[0].KeyFound {
		t.Errorf("Delete result: got %+v, want prev=gone found=true", results[0])
	}
	h.CheckGetNotFound(c, "k")
}

func TestTxnWatchEvents(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

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

	success := []api.TxnOp{
		{Op: api.TxnOpPut, Key: "a", Value: "1"},
		{Op: api.TxnOpPut, Key: "b", Value: "2"},
	}
	_, _, _, err = c.Txn(ctx, nil, success, nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, wantKey := range []string{"a", "b"} {
		select {
		case ev := <-ch:
			if ev.Key != wantKey {
				t.Errorf("got key %q, want %q", ev.Key, wantKey)
			}
			if ev.Type != api.EventPut {
				t.Errorf("expected EventPut, got %v", ev.Type)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for event %s", wantKey)
		}
	}

	success2 := []api.TxnOp{
		{Op: api.TxnOpDelete, Key: "a"},
	}
	_, _, _, err = c.Txn(ctx, nil, success2, nil)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-ch:
		if ev.Key != "a" || ev.Type != api.EventDelete {
			t.Errorf("Delete event: got %+v, want key=a type=delete", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delete event")
	}

	conds := []api.TxnCondition{
		{Key: "b", Compare: api.CompareEQ, Value: "2"},
	}
	success3 := []api.TxnOp{
		{Op: api.TxnOpCAS, Key: "b", CompareValue: "2", Value: "3"},
	}
	failure3 := []api.TxnOp{
		{Op: api.TxnOpPut, Key: "b", Value: "wrong"},
	}
	_, _, _, err = c.Txn(ctx, conds, success3, failure3)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-ch:
		if ev.Key != "b" || ev.Value != "3" {
			t.Errorf("CAS event: got %+v, want key=b value=3", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CAS event")
	}

	conds4 := []api.TxnCondition{
		{Key: "nonexistent", Compare: api.CompareNotExists},
	}
	success4 := []api.TxnOp{
		{Op: api.TxnOpPut, Key: "c", Value: "hello"},
		{Op: api.TxnOpAppend, Key: "c", Value: " world"},
	}
	_, _, _, err = c.Txn(ctx, conds4, success4, nil)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-ch:
		if ev.Key != "c" || ev.Value != "hello" {
			t.Errorf("Put event: got %+v, want key=c value=hello", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Put c event")
	}

	select {
	case ev := <-ch:
		if ev.Key != "c" || ev.Value != "hello world" {
			t.Errorf("Append event: got %+v, want key=c value='hello world'", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Append c event")
	}
}

func TestTxnDuplicate(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	leader := h.CheckSingleLeader()
	addr := h.kvServiceAddrs[leader]

	// Build the request body with explicit ClientID/RequestID
	req := api.TxnRequest{
		Success: []api.TxnOp{
			{Op: api.TxnOpPut, Key: "k", Value: "v"},
		},
		ClientID:  99,
		RequestID: 1,
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	// First call should succeed
	url := fmt.Sprintf("http://%s/txn/", addr)
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var firstResp api.TxnResponse
	if err := json.NewDecoder(resp.Body).Decode(&firstResp); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if firstResp.RespStatus != api.StatusOK {
		t.Fatalf("first call: expected StatusOK, got %v", firstResp.RespStatus)
	}

	// Second call with same ClientID/RequestID should be duplicate
	resp, err = http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var secondResp api.TxnResponse
	if err := json.NewDecoder(resp.Body).Decode(&secondResp); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if secondResp.RespStatus != api.StatusDuplicateRequest {
		t.Fatalf("second call: expected StatusDuplicateRequest, got %v", secondResp.RespStatus)
	}
}

func TestTxnCompareOps(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()

	h.CheckPut(c, "n", "5")
	h.CheckPut(c, "s", "foo")

	tests := []struct {
		name   string
		cond   api.TxnCondition
		wantOK bool
	}{
		{"CompareEQ", api.TxnCondition{Key: "s", Compare: api.CompareEQ, Value: "foo"}, true},
		{"CompareNEQ", api.TxnCondition{Key: "s", Compare: api.CompareNEQ, Value: "bar"}, true},
		{"CompareGTE-int", api.TxnCondition{Key: "n", Compare: api.CompareGTE, Value: "3"}, true},
		{"CompareGTE-fail", api.TxnCondition{Key: "n", Compare: api.CompareGTE, Value: "10"}, false},
		{"CompareLTE-int", api.TxnCondition{Key: "n", Compare: api.CompareLTE, Value: "5"}, true},
		{"CompareLTE-fail", api.TxnCondition{Key: "n", Compare: api.CompareLTE, Value: "3"}, false},
		{"CompareExists", api.TxnCondition{Key: "s", Compare: api.CompareExists}, true},
		{"CompareNotExists", api.TxnCondition{Key: "missing", Compare: api.CompareNotExists}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			success := []api.TxnOp{
				{Op: api.TxnOpPut, Key: "out", Value: "success"},
			}
			failure := []api.TxnOp{
				{Op: api.TxnOpPut, Key: "out", Value: "failure"},
			}
			succeeded, _, _, err := c.Txn(ctx, []api.TxnCondition{tt.cond}, success, failure)
			if err != nil {
				t.Fatal(err)
			}
			if succeeded != tt.wantOK {
				t.Errorf("expected succeeded=%v, got %v", tt.wantOK, succeeded)
			}
		})
	}
}
