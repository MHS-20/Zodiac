package test

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/MHS-20/Raft/raft"
	"github.com/MHS-20/Zodiac/kvclient"
	"github.com/MHS-20/Zodiac/kvservice"
)

func freePort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

type Harness struct {
	n int

	kvCluster     []*kvservice.KVService
	kvServiceAddrs []string

	storage []*raft.MapStorage

	t *testing.T

	connected []bool
	alive     []bool

	ctx       context.Context
	ctxCancel func()
}

func NewHarness(t *testing.T, n int) *Harness {
	return NewHarnessWithPort(t, n, 0)
}

func NewHarnessWithPort(t *testing.T, n int, basePort int) *Harness {
	kvss := make([]*kvservice.KVService, n)
	ready := make(chan any)
	connected := make([]bool, n)
	alive := make([]bool, n)
	storage := make([]*raft.MapStorage, n)

	for i := range n {
		peerIds := make([]int, 0)
		for p := range n {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		storage[i] = raft.NewMapStorage()
		kvss[i] = kvservice.New(i, peerIds, storage[i], ready)
		alive[i] = true
	}

	for i := range n {
		for j := range n {
			if i != j {
				kvss[i].ConnectToRaftPeer(j, kvss[j].GetRaftListenAddr())
			}
		}
		connected[i] = true
	}
	close(ready)

	kvServiceAddrs := make([]string, n)
	for i := range n {
		var port int
		if basePort > 0 {
			port = basePort + i
		} else {
			port = freePort()
		}
		kvss[i].ServeHTTP(port)

		kvServiceAddrs[i] = fmt.Sprintf("localhost:%d", port)
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	h := &Harness{
		n:              n,
		kvCluster:      kvss,
		kvServiceAddrs: kvServiceAddrs,
		t:              t,
		connected:      connected,
		alive:          alive,
		storage:        storage,
		ctx:            ctx,
		ctxCancel:      ctxCancel,
	}
	return h
}

func (h *Harness) DisconnectServiceFromPeers(id int) {
	tlog("Disconnect %d", id)
	h.kvCluster[id].DisconnectFromAllRaftPeers()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.kvCluster[j].DisconnectFromRaftPeer(id)
		}
	}
	h.connected[id] = false
}

func (h *Harness) ReconnectServiceToPeers(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id && h.alive[j] {
			if err := h.kvCluster[id].ConnectToRaftPeer(j, h.kvCluster[j].GetRaftListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.kvCluster[j].ConnectToRaftPeer(id, h.kvCluster[id].GetRaftListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[id] = true
}

func (h *Harness) CrashService(id int) {
	tlog("Crash %d", id)
	h.DisconnectServiceFromPeers(id)
	h.alive[id] = false
	if err := h.kvCluster[id].Shutdown(); err != nil {
		h.t.Errorf("error while shutting down service %d: %v", id, err)
	}
}

func (h *Harness) RestartService(id int) {
	if h.alive[id] {
		log.Fatalf("id=%d is alive in RestartService", id)
	}
	tlog("Restart %d", id)

	peerIds := make([]int, 0)
	for p := range h.n {
		if p != id {
			peerIds = append(peerIds, p)
		}
	}
	ready := make(chan any)
	h.kvCluster[id] = kvservice.New(id, peerIds, h.storage[id], ready)
	port := freePort()
	h.kvCluster[id].ServeHTTP(port)
	h.kvServiceAddrs[id] = fmt.Sprintf("localhost:%d", port)

	h.ReconnectServiceToPeers(id)
	close(ready)
	h.alive[id] = true
	time.Sleep(20 * time.Millisecond)
}

func (h *Harness) DelayNextHTTPResponseFromService(id int) {
	tlog("Delaying next HTTP response from %d", id)
	h.kvCluster[id].DelayNextHTTPResponse()
}

func (h *Harness) Shutdown() {
	for i := range h.n {
		h.kvCluster[i].DisconnectFromAllRaftPeers()
		h.connected[i] = false
	}

	http.DefaultClient.CloseIdleConnections()
	h.ctxCancel()

	for i := range h.n {
		if h.alive[i] {
			h.alive[i] = false
			if err := h.kvCluster[i].Shutdown(); err != nil {
				h.t.Errorf("error while shutting down service %d: %v", i, err)
			}
		}
	}
}

func (h *Harness) NewClient() *kvclient.KVClient {
	var addrs []string
	for i := range h.n {
		if h.alive[i] {
			addrs = append(addrs, h.kvServiceAddrs[i])
		}
	}
	return kvclient.New(addrs)
}

func (h *Harness) NewClientWithRandomAddrsOrder() *kvclient.KVClient {
	var addrs []string
	for i := range h.n {
		if h.alive[i] {
			addrs = append(addrs, h.kvServiceAddrs[i])
		}
	}
	rand.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})
	return kvclient.New(addrs)
}

func (h *Harness) NewClientSingleService(id int) *kvclient.KVClient {
	addrs := h.kvServiceAddrs[id : id+1]
	return kvclient.New(addrs)
}

func (h *Harness) CheckSingleLeader() int {
	for r := 0; r < 8; r++ {
		leaderId := -1
		for i := range h.n {
			if h.connected[i] && h.kvCluster[i].IsLeader() {
				if leaderId < 0 {
					leaderId = i
				} else {
					h.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
				}
			}
		}
		if leaderId >= 0 {
			return leaderId
		}
		time.Sleep(150 * time.Millisecond)
	}

	h.t.Fatalf("leader not found")
	return -1
}

func (h *Harness) CheckPutWithLease(c *kvclient.KVClient, key, value string, leaseID int64) (string, bool) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	pv, f, _, err := c.PutWithLease(ctx, key, value, leaseID)
	if err != nil {
		h.t.Error(err)
	}
	return pv, f
}

func (h *Harness) CheckPut(c *kvclient.KVClient, key, value string) (string, bool) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	pv, f, _, err := c.Put(ctx, key, value)
	if err != nil {
		h.t.Error(err)
	}
	return pv, f
}

func (h *Harness) CheckAppend(c *kvclient.KVClient, key, value string) (string, bool) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	pv, f, _, err := c.Append(ctx, key, value)
	if err != nil {
		h.t.Error(err)
	}
	return pv, f
}

func (h *Harness) CheckGet(c *kvclient.KVClient, key string, wantValue string) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	gv, f, _, err := c.Get(ctx, key)
	if err != nil {
		h.t.Error(err)
	}
	if !f {
		h.t.Errorf("got found=false, want true for key=%s", key)
	}
	if gv != wantValue {
		h.t.Errorf("got value=%v, want %v", gv, wantValue)
	}
}

func (h *Harness) CheckCAS(c *kvclient.KVClient, key, compare, value string) (string, bool) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	pv, f, _, err := c.CAS(ctx, key, compare, value)
	if err != nil {
		h.t.Error(err)
	}
	return pv, f
}

func (h *Harness) CheckGetNotFound(c *kvclient.KVClient, key string) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	_, f, _, err := c.Get(ctx, key)
	if err != nil {
		h.t.Error(err)
	}
	if f {
		h.t.Errorf("got found=true, want false for key=%s", key)
	}
}

func (h *Harness) CheckGetTimesOut(c *kvclient.KVClient, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, _, err := c.Get(ctx, key)
	if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		h.t.Errorf("got err %v; want 'deadline exceeded'", err)
	}
}

func (h *Harness) AddNodeToCluster(leaderId int, newId int) {
	tlog("AddNodeToCluster: leader=%d new=%d", leaderId, newId)
	raftSrv := h.kvCluster[leaderId].RaftServer()
	addr := h.kvCluster[newId].GetRaftListenAddr()
	ok := raftSrv.AddPeer(newId, addr)
	if !ok {
		h.t.Fatalf("AddNodeToCluster: AddPeer(%d) from leader %d returned false", newId, leaderId)
	}
	h.n = max(h.n, newId+1)
	for len(h.connected) <= newId {
		h.connected = append(h.connected, true)
	}
	for len(h.alive) <= newId {
		h.alive = append(h.alive, true)
	}
}

func (h *Harness) RemoveNodeFromCluster(leaderId int, targetId int) {
	tlog("RemoveNodeFromCluster: leader=%d target=%d", leaderId, targetId)
	raftSrv := h.kvCluster[leaderId].RaftServer()
	ok := raftSrv.RemovePeer(targetId)
	if !ok {
		h.t.Fatalf("RemoveNodeFromCluster: RemovePeer(%d) from leader %d returned false", targetId, leaderId)
	}
	if targetId < len(h.connected) {
		h.connected[targetId] = false
	}
}

func (h *Harness) CheckPeerList(id int, wantPeers []int) {
	h.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got := h.kvCluster[id].PeerIDs()
		if sameIntSet(got, wantPeers) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	got := h.kvCluster[id].PeerIDs()
	h.t.Errorf("server %d peerIds = %v; want %v", id, got, wantPeers)
}

func sameIntSet(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	count := make(map[int]int, len(a))
	for _, v := range a {
		count[v]++
	}
	for _, v := range b {
		count[v]--
		if count[v] < 0 {
			return false
		}
	}
	return true
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

func tlog(format string, a ...any) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}
