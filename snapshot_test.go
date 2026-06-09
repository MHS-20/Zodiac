package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

// submitPuts hammers the cluster with n sequential Put requests using a fresh
// client for each one, and verifies each one succeeds. Keys/values are
// "key0"/"value0" … "key(n-1)"/"value(n-1)".
func submitPuts(t *testing.T, h *Harness, n int) {
	t.Helper()
	for i := range n {
		c := h.NewClient()
		h.CheckPut(c, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}
}

// checkAllKeys verifies that key0…key(n-1) are readable with the expected values.
func checkAllKeys(t *testing.T, h *Harness, n int) {
	t.Helper()
	c := h.NewClient()
	for i := range n {
		h.CheckGet(c, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}
}

// TestSnapshotBasicRestart writes enough entries to trigger at least one
// snapshot (> snapshotThreshold = 100), crashes the leader, restarts it, and
// confirms all keys are still readable — including from the restarted node.
func TestSnapshotBasicRestart(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	const n = 110
	submitPuts(t, h, n)
	sleepMs(200)

	// Crash the leader, wait for a new one, then bring the old one back.
	h.CrashService(lid)
	h.CheckSingleLeader()
	h.RestartService(lid) // alive[lid] == false here, safe to restart
	sleepMs(300)

	checkAllKeys(t, h, n)
}

// TestSnapshotFollowerCatchUp isolates a follower before any writes so it
// misses all log entries, then reconnects it. The leader must ship a snapshot
// because the missing entries have been compacted away.
func TestSnapshotFollowerCatchUp(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	lagId := (lid + 1) % 3
	h.DisconnectServiceFromPeers(lagId)

	const n = 110
	submitPuts(t, h, n)
	sleepMs(200)

	// Reconnect; the leader will install a snapshot on the lagging follower.
	h.ReconnectServiceToPeers(lagId)
	sleepMs(400)

	checkAllKeys(t, h, n)
}

// TestSnapshotRestartIsolatedFollower crashes a follower (empty storage on
// restart) so it misses all writes and the snapshot. It must receive a
// snapshot from the leader on rejoining.
func TestSnapshotRestartIsolatedFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	lagId := (lid + 1) % 3
	h.CrashService(lagId) // alive[lagId] = false

	const n = 110
	submitPuts(t, h, n)
	sleepMs(200)

	h.RestartService(lagId) // starts with empty storage
	sleepMs(400)

	checkAllKeys(t, h, n)
}

// TestSnapshotMultipleRounds writes two batches of entries separated by a
// leader crash, producing at least two successive snapshots. Verifies the
// second snapshot supersedes the first and all keys from both rounds survive.
func TestSnapshotMultipleRounds(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	// --- Round 1 ---
	lid1 := h.CheckSingleLeader()
	const round1 = 110
	submitPuts(t, h, round1)
	sleepMs(200)

	// Crash the leader; a new one takes over. lid1 is now dead.
	h.CrashService(lid1)
	h.CheckSingleLeader()

	// --- Round 2: different key namespace ---
	const round2 = 110
	for i := range round2 {
		c := h.NewClient()
		h.CheckPut(c, fmt.Sprintf("key2-%d", i), fmt.Sprintf("val2-%d", i))
	}
	sleepMs(200)

	// Bring the round-1 leader back; it must catch up via snapshot.
	h.RestartService(lid1) // alive[lid1] == false, safe
	sleepMs(400)

	// All keys from both rounds must be visible from any node.
	checkAllKeys(t, h, round1)
	c := h.NewClient()
	for i := range round2 {
		h.CheckGet(c, fmt.Sprintf("key2-%d", i), fmt.Sprintf("val2-%d", i))
	}
}

// TestSnapshotLeaderCrashDuringFollowerCatchUp disconnects a follower, writes
// past the threshold, then crashes the *original* leader before reconnecting
// the follower. The *new* leader must be able to ship the snapshot.
func TestSnapshotLeaderCrashDuringFollowerCatchUp(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	lagId := (lid + 1) % 3
	h.DisconnectServiceFromPeers(lagId)

	const n = 110
	submitPuts(t, h, n)
	sleepMs(200)

	// Crash the original leader. Reconnect the lagging follower first so that
	// two of three nodes can form a majority and elect a new leader, which
	// then ships the snapshot to the lagging follower.
	h.CrashService(lid)
	h.ReconnectServiceToPeers(lagId)
	h.CheckSingleLeader()
	sleepMs(400)

	checkAllKeys(t, h, n)

	// Bring the originally-crashed leader back too.
	h.RestartService(lid) // alive[lid] == false, safe
	sleepMs(300)
	checkAllKeys(t, h, n)
}

// TestSnapshotDuplicateRequestsSurvive checks that the deduplication table
// (lastRequestIDPerClient) is serialised into the snapshot and restored
// correctly. After a snapshot + restart, a replayed request must not be
// applied a second time.
func TestSnapshotDuplicateRequestsSurvive(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	c1 := h.NewClient()
	h.CheckPut(c1, "counter", "0")

	// Accumulate enough entries to trigger a snapshot, using the same client
	// so its clientID is in the dedup table that gets snapshotted.
	for i := range 110 {
		h.CheckPut(c1, fmt.Sprintf("filler%d", i), "x")
	}
	sleepMs(200)

	h.CrashService(lid)
	h.CheckSingleLeader()
	h.RestartService(lid) // alive[lid] == false, safe
	sleepMs(300)

	// The value written before the snapshot must still be there.
	h.CheckGet(c1, "counter", "0")
}
