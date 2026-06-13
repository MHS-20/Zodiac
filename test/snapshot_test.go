package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func submitPuts(t *testing.T, h *Harness, n int) {
	t.Helper()
	for i := range n {
		c := h.NewClient()
		h.CheckPut(c, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}
}

func checkAllKeys(t *testing.T, h *Harness, n int) {
	t.Helper()
	c := h.NewClient()
	for i := range n {
		h.CheckGet(c, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}
}

func TestSnapshotBasicRestart(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	const n = 110
	submitPuts(t, h, n)
	sleepMs(200)

	h.CrashService(lid)
	h.CheckSingleLeader()
	h.RestartService(lid)
	sleepMs(300)

	checkAllKeys(t, h, n)
}

func TestSnapshotFollowerCatchUp(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	lagId := (lid + 1) % 3
	h.DisconnectServiceFromPeers(lagId)

	const n = 110
	submitPuts(t, h, n)
	sleepMs(200)

	h.ReconnectServiceToPeers(lagId)
	sleepMs(400)

	checkAllKeys(t, h, n)
}

func TestSnapshotRestartIsolatedFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	lagId := (lid + 1) % 3
	h.CrashService(lagId)

	const n = 110
	submitPuts(t, h, n)
	sleepMs(200)

	h.RestartService(lagId)
	sleepMs(400)

	checkAllKeys(t, h, n)
}

func TestSnapshotMultipleRounds(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid1 := h.CheckSingleLeader()
	const round1 = 110
	submitPuts(t, h, round1)
	sleepMs(200)

	h.CrashService(lid1)
	h.CheckSingleLeader()

	const round2 = 110
	for i := range round2 {
		c := h.NewClient()
		h.CheckPut(c, fmt.Sprintf("key2-%d", i), fmt.Sprintf("val2-%d", i))
	}
	sleepMs(200)

	h.RestartService(lid1)
	sleepMs(400)

	checkAllKeys(t, h, round1)
	c := h.NewClient()
	for i := range round2 {
		h.CheckGet(c, fmt.Sprintf("key2-%d", i), fmt.Sprintf("val2-%d", i))
	}
}

func TestSnapshotLeaderCrashDuringFollowerCatchUp(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	lagId := (lid + 1) % 3
	h.DisconnectServiceFromPeers(lagId)

	const n = 110
	submitPuts(t, h, n)
	sleepMs(200)

	h.CrashService(lid)
	h.ReconnectServiceToPeers(lagId)
	h.CheckSingleLeader()
	sleepMs(400)

	checkAllKeys(t, h, n)

	h.RestartService(lid)
	sleepMs(300)
	checkAllKeys(t, h, n)
}

func TestSnapshotDuplicateRequestsSurvive(t *testing.T) {
	defer leaktest.CheckTimeout(t, 500*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	c1 := h.NewClient()
	h.CheckPut(c1, "counter", "0")

	for i := range 110 {
		h.CheckPut(c1, fmt.Sprintf("filler%d", i), "x")
	}
	sleepMs(200)

	h.CrashService(lid)
	h.CheckSingleLeader()
	h.RestartService(lid)
	sleepMs(300)

	h.CheckGet(c1, "counter", "0")
}
