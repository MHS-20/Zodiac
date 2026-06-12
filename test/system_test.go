package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func TestSetupHarness(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	sleepMs(80)
}

func TestClientRequestBeforeConsensus(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	sleepMs(10)

	c1 := h.NewClient()
	h.CheckPut(c1, "llave", "cosa")
	sleepMs(80)
}

func TestBasicPutGetSingleClient(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := h.NewClient()
	h.CheckPut(c1, "llave", "cosa")

	h.CheckGet(c1, "llave", "cosa")
	sleepMs(80)
}

func TestPutPrevValue(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := h.NewClient()
	prev, found := h.CheckPut(c1, "llave", "cosa")
	if found || prev != "" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}

	prev, found = h.CheckPut(c1, "llave", "frodo")
	if !found || prev != "cosa" {
		t.Errorf(`got found=%v, prev=%v, want true/"cosa"`, found, prev)
	}

	prev, found = h.CheckPut(c1, "mafteah", "davar")
	if found || prev != "" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}
}

func TestBasicAppendSameClient(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := h.NewClient()
	h.CheckPut(c1, "foo", "bar")

	prev, found := h.CheckAppend(c1, "foo", "baz")
	if !found || prev != "bar" {
		t.Errorf(`got found=%v, prev=%v, want true/"foo"`, found, prev)
	}
	h.CheckGet(c1, "foo", "barbaz")

	prev, found = h.CheckAppend(c1, "mix", "match")
	if found || prev != "" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}
	h.CheckGet(c1, "mix", "match")
}

func TestBasicPutGetDifferentClients(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := h.NewClient()
	h.CheckPut(c1, "k", "v")

	c2 := h.NewClient()
	h.CheckGet(c2, "k", "v")
	sleepMs(80)
}

func TestBasicAppendDifferentClients(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := h.NewClient()
	h.CheckPut(c1, "foo", "bar")

	c2 := h.NewClient()
	prev, found := h.CheckAppend(c2, "foo", "baz")
	if !found || prev != "bar" {
		t.Errorf(`got found=%v, prev=%v, want true/"foo"`, found, prev)
	}
	h.CheckGet(c1, "foo", "barbaz")

	prev, found = h.CheckAppend(c2, "mix", "match")
	if found || prev != "" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}
	h.CheckGet(c1, "mix", "match")
}

func TestAppendDifferentLeaders(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	c1 := h.NewClient()
	h.CheckAppend(c1, "foo", "bar")
	h.CheckGet(c1, "foo", "bar")

	h.CrashService(lid)
	h.CheckSingleLeader()

	c2 := h.NewClient()
	h.CheckAppend(c2, "foo", "baz")
	h.CheckGet(c2, "foo", "barbaz")

	h.RestartService(lid)
	c3 := h.NewClient()
	sleepMs(300)
	h.CheckGet(c3, "foo", "barbaz")
}

func TestCASBasic(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := h.NewClient()
	h.CheckPut(c1, "k", "v")

	if pv, found := h.CheckCAS(c1, "k", "v", "newv"); pv != "v" || !found {
		t.Errorf("got %s,%v, want replacement", pv, found)
	}
}

func TestCASConcurrent(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()
	c := h.NewClient()
	h.CheckPut(c, "foo", "mexico")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c := h.NewClient()
		for range 20 {
			h.CheckCAS(c, "foo", "bar", "bomba")
		}
	}()

	sleepMs(50)
	c2 := h.NewClient()
	h.CheckPut(c2, "foo", "bar")

	sleepMs(300)
	h.CheckGet(c2, "foo", "bomba")

	wg.Wait()
}

func TestConcurrentClientsPutsAndGets(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	n := 9
	for i := range n {
		go func() {
			c := h.NewClient()
			_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
			if f {
				t.Errorf("got key found for %d, want false", i)
			}
		}()
	}
	sleepMs(150)

	for i := range n {
		go func() {
			c := h.NewClient()
			h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		}()
	}
	sleepMs(150)
}

func Test5ServerConcurrentClientsPutsAndGets(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 5)
	defer h.Shutdown()
	h.CheckSingleLeader()

	n := 9
	for i := range n {
		go func() {
			c := h.NewClient()
			_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
			if f {
				t.Errorf("got key found for %d, want false", i)
			}
		}()
	}
	sleepMs(150)

	for i := range n {
		go func() {
			c := h.NewClient()
			h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		}()
	}
	sleepMs(150)
}

func TestDisconnectLeaderAfterPuts(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	n := 4
	for i := range n {
		c := h.NewClient()
		h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}

	h.DisconnectServiceFromPeers(lid)
	sleepMs(300)
	newlid := h.CheckSingleLeader()

	if newlid == lid {
		t.Errorf("got the same leader")
	}

	c := h.NewClientSingleService(lid)
	h.CheckGetTimesOut(c, "key1")

	for range 5 {
		c := h.NewClientWithRandomAddrsOrder()
		for j := range n {
			h.CheckGet(c, fmt.Sprintf("key%v", j), fmt.Sprintf("value%v", j))
		}
	}

	h.ReconnectServiceToPeers(lid)
	sleepMs(200)
}

func TestDisconnectLeaderAndFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	n := 4
	for i := range n {
		c := h.NewClient()
		_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		if f {
			t.Errorf("got key found for %d, want false", i)
		}
	}

	h.DisconnectServiceFromPeers(lid)
	otherId := (lid + 1) % 3
	h.DisconnectServiceFromPeers(otherId)
	sleepMs(100)

	c := h.NewClient()
	h.CheckGetTimesOut(c, "key0")

	h.ReconnectServiceToPeers(otherId)
	h.CheckSingleLeader()
	for i := range n {
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}

	h.ReconnectServiceToPeers(lid)
	h.CheckSingleLeader()
	for i := range n {
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}
	sleepMs(100)
}

func TestCrashFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	n := 3
	for i := range n {
		c := h.NewClient()
		_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		if f {
			t.Errorf("got key found for %d, want false", i)
		}
	}

	otherId := (lid + 1) % 3
	h.CrashService(otherId)

	for i := range n {
		c := h.NewClientSingleService(lid)
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}

	for i := range n {
		c := h.NewClient()
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}
}

func TestCrashLeader(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	n := 3
	for i := range n {
		c := h.NewClient()
		_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		if f {
			t.Errorf("got key found for %d, want false", i)
		}
	}

	h.CrashService(lid)
	h.CheckSingleLeader()

	for i := range n {
		c := h.NewClient()
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}
}

func TestCrashThenRestartLeader(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	n := 3
	for i := range n {
		c := h.NewClient()
		_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		if f {
			t.Errorf("got key found for %d, want false", i)
		}
	}

	h.CrashService(lid)
	h.CheckSingleLeader()

	for i := range n {
		c := h.NewClient()
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}

	h.RestartService(lid)

	for range 5 {
		c := h.NewClientWithRandomAddrsOrder()
		for j := range n {
			h.CheckGet(c, fmt.Sprintf("key%v", j), fmt.Sprintf("value%v", j))
		}
	}
}

func TestAppendLinearizableAfterDelay(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	c1 := h.NewClient()

	h.CheckPut(c1, "foo", "bar")
	h.CheckAppend(c1, "foo", "baz")
	h.CheckGet(c1, "foo", "barbaz")

	h.DelayNextHTTPResponseFromService(lid)

	_, _, err := c1.Append(context.Background(), "foo", "mira")
	if err == nil {
		t.Errorf("got no error, want duplicate")
	}

	sleepMs(300)
	h.CheckGet(c1, "foo", "barbazmira")
}

func TestAppendLinearizableAfterCrash(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	c1 := h.NewClient()

	h.CheckAppend(c1, "foo", "bar")
	h.CheckGet(c1, "foo", "bar")

	h.DelayNextHTTPResponseFromService(lid)
	go func() {
		ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
		defer cancel()
		_, _, err := c1.Append(ctx, "foo", "mira")
		if err == nil {
			t.Errorf("got no error; want error")
		}
		tlog("received err: %v", err)
	}()

	sleepMs(50)
	h.CrashService(lid)
	h.CheckSingleLeader()
	c2 := h.NewClient()
	h.CheckGet(c2, "foo", "barmira")
}

func TestListBasic(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c := h.NewClient()
	h.CheckPut(c, "/nodes/1/cpu", "4")
	h.CheckPut(c, "/nodes/1/mem", "16")
	h.CheckPut(c, "/nodes/2/cpu", "8")
	h.CheckPut(c, "/pods/a", "running")

	pairs, err := c.List(context.Background(), "/nodes/1/")
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 2 || pairs["/nodes/1/cpu"] != "4" || pairs["/nodes/1/mem"] != "16" {
		t.Errorf("got %v, want 2 entries under /nodes/1/", pairs)
	}

	pairs, err = c.List(context.Background(), "/nonexistent/")
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 0 {
		t.Errorf("got %d entries, want 0", len(pairs))
	}
}

func TestListAcrossCluster(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	c1 := h.NewClientSingleService(lid)
	c1.Put(context.Background(), "/nodes/1/cpu", "4")
	c1.Put(context.Background(), "/nodes/1/mem", "16")

	h.CrashService(lid)
	newLid := h.CheckSingleLeader()

	c2 := h.NewClientSingleService(newLid)
	pairs, err := c2.List(context.Background(), "/nodes/")
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 2 {
		t.Errorf("got %d entries, want 2: %v", len(pairs), pairs)
	}
	if pairs["/nodes/1/cpu"] != "4" || pairs["/nodes/1/mem"] != "16" {
		t.Errorf("unexpected values: %v", pairs)
	}

	h.RestartService(lid)
}
