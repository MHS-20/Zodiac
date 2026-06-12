package kvservice

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MHS-20/Raft/raft"
	"github.com/MHS-20/Zodiac/api"
)

// snapshotThreshold is the number of committed log entries between snapshots.
// Tune this value to trade off memory/disk usage against snapshotting overhead.
const (
	snapshotThreshold = 100
	peerStoreKey      = "kvpeers"
)

// kvSnapshot is the serialisable state that gets stored in a Raft snapshot.
type kvSnapshot struct {
	Store                  map[string]string
	LastRequestIDPerClient map[int64]int64
	CurrentRevision        int64
}

// peerInfo tracks the addresses of a cluster peer for reconnection and discovery.
type peerInfo struct {
	RaftAddr string
	HTTPAddr string
}

// persistedPeer is the JSON-serialisable form of a peer record.
type persistedPeer struct {
	ID       int    `json:"id"`
	RaftAddr string `json:"raft_addr"`
	HTTPAddr string `json:"http_addr"`
}

type KVService struct {
	sync.Mutex
	id     int
	rs     *raft.Server
	logger *slog.Logger

	// commitChan is the commit channel passed to the Raft server; when commands
	// are committed, they're sent on this channel.
	commitChan chan raft.CommitEntry

	// commitSubs are the commit subscriptions currently active in this service.
	// See the createCommitSubscription method for more details.
	commitSubs map[int]chan Command

	ds                     *DataStore
	srv                    *http.Server
	lastRequestIDPerClient map[int64]int64
	currentRevision        int64
	delayNextHTTPResponse  atomic.Bool

	// lastSnapshotIndex tracks the log index of the most recent snapshot so we
	// know how many entries have accumulated since the last compaction.
	lastSnapshotIndex int

	// In-cluster peer address book: ID → {Raft,HTTP} addresses.
	// Updated when ConfigChange entries commit and persisted to storage.
	peers map[int]peerInfo

	// peerIDs is the current Raft peer list (parallels raft.CM.peerIds).
	// Updated on config changes so we can expose PeerIDs() without poking
	// into the unexported raft module fields.
	peerIDs []int

	// Pending addresses for nodes that have been submitted via /join/ but whose
	// ConfigChange entry hasn't committed yet.  Moved to peers on commit.
	pendingJoinAddrs map[int]peerInfo

	storage raft.Storage

	// Watch subsystem: event buffer + subscription manager
	eventBuf  *eventBuffer
	watchSubs map[int64]*watchSub
	watchMu   sync.Mutex
	watchSeq  int64
}

func New(id int, peerIds []int, storage raft.Storage, readyChan <-chan any) *KVService {
	gob.Register(Command{})
	commitChan := make(chan raft.CommitEntry)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})).With("component", "kv", "id", id)

	rs := raft.NewServer(id, peerIds, storage, readyChan, (chan<- raft.CommitEntry)(commitChan), logger)
	rs.Serve()
	kvs := &KVService{
		id:                     id,
		rs:                     rs,
		logger:                 logger,
		commitChan:             commitChan,
		ds:                     NewDataStore(),
		commitSubs:             make(map[int]chan Command),
		lastRequestIDPerClient: make(map[int64]int64),
		lastSnapshotIndex:      -1,
		peers:                  make(map[int]peerInfo),
		peerIDs:                append([]int(nil), peerIds...),
		pendingJoinAddrs:       make(map[int]peerInfo),
		storage:                storage,
		currentRevision:        0,
		eventBuf:               newEventBuffer(eventBufferSize),
		watchSubs:              make(map[int64]*watchSub),
	}

	kvs.loadPeers()
	kvs.runUpdater()
	return kvs
}

func (kvs *KVService) IsLeader() bool {
	return kvs.rs.IsLeader()
}

func (kvs *KVService) ServeHTTP(port int) {
	if kvs.srv != nil {
		panic("ServeHTTP called with existing server")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /get/", kvs.handleGet)
	mux.HandleFunc("POST /put/", kvs.handlePut)
	mux.HandleFunc("POST /append/", kvs.handleAppend)
	mux.HandleFunc("POST /cas/", kvs.handleCAS)
	mux.HandleFunc("POST /list/", kvs.handleList)
	mux.HandleFunc("POST /join/", kvs.handleJoin)
	mux.HandleFunc("POST /leave/", kvs.handleLeave)
	mux.HandleFunc("GET /status/", kvs.handleStatus)
	mux.HandleFunc("GET /members/", kvs.handleMembers)
	mux.HandleFunc("GET /watch/", kvs.handleWatch)

	kvs.srv = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		kvs.logger.Info("serving HTTP", "addr", kvs.srv.Addr)
		if err := kvs.srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
		kvs.srv = nil
	}()
}

func (kvs *KVService) Shutdown() error {
	kvs.logger.Debug("shutting down Raft server")
	kvs.rs.Shutdown()
	kvs.logger.Debug("closing commitChan")
	close(kvs.commitChan)

	if kvs.srv != nil {
		kvs.logger.Debug("shutting down HTTP server")
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		kvs.srv.Shutdown(ctx)
		kvs.logger.Debug("HTTP shutdown complete")
		return nil
	}

	return nil
}

func (kvs *KVService) DelayNextHTTPResponse() {
	kvs.delayNextHTTPResponse.Store(true)
}

func (kvs *KVService) sendHTTPResponse(w http.ResponseWriter, v any) {
	if kvs.delayNextHTTPResponse.Load() {
		kvs.delayNextHTTPResponse.Store(false)
		time.Sleep(300 * time.Millisecond)
	}
	kvs.logger.Debug("sending response", "value", fmt.Sprintf("%#v", v))
	renderJSON(w, v)
}

func (kvs *KVService) handlePut(w http.ResponseWriter, req *http.Request) {
	pr := &api.PutRequest{}
	if err := readRequestJSON(req, pr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.logger.Debug("HTTP PUT", "request", pr)

	// Submit a command into the Raft server; this is the state change in the
	// replicated state machine built on top of the Raft log.
	cmd := Command{
		Kind:      CommandPut,
		Key:       pr.Key,
		Value:     pr.Value,
		ServiceID: kvs.id,
		ClientID:  pr.ClientID,
		RequestID: pr.RequestID,
	}
	result := kvs.rs.Submit(cmd)
	// If we're not the Raft leader, send an appropriate status
	if result.Index < 0 {
		kvs.sendHTTPResponse(w, api.PutResponse{RespStatus: api.StatusNotLeader})
		return
	}

	// Subscribe for a commit update for our log index
	sub := kvs.createCommitSubscription(result.Index)

	select {
	case commitCmd, ok := <-sub:
		if !ok {
			// Subscription was cancelled because a snapshot covered this index.
			kvs.sendHTTPResponse(w, api.PutResponse{RespStatus: api.StatusFailedCommit})
			return
		}
		if commitCmd.ServiceID == kvs.id {
			if commitCmd.IsDuplicate {
				// If this command is a duplicate, it wasn't executed as a result of
				// this request. Notify the client with a special status.
				kvs.sendHTTPResponse(w, api.PutResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				kvs.sendHTTPResponse(w, api.PutResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					PrevValue:  commitCmd.ResultValue,
					Revision:   commitCmd.Revision,
				})
			}
		} else {
			// leadership lost
			kvs.sendHTTPResponse(w, api.PutResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

func (kvs *KVService) handleAppend(w http.ResponseWriter, req *http.Request) {
	ar := &api.AppendRequest{}
	if err := readRequestJSON(req, ar); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.logger.Debug("HTTP APPEND", "request", ar)

	cmd := Command{
		Kind:      CommandAppend,
		Key:       ar.Key,
		Value:     ar.Value,
		ServiceID: kvs.id,
		ClientID:  ar.ClientID,
		RequestID: ar.RequestID,
	}
	result := kvs.rs.Submit(cmd)
	if result.Index < 0 {
		kvs.sendHTTPResponse(w, api.AppendResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := kvs.createCommitSubscription(result.Index)

	select {
	case commitCmd, ok := <-sub:
		if !ok {
			// Subscription was cancelled because a snapshot covered this index.
			kvs.sendHTTPResponse(w, api.AppendResponse{RespStatus: api.StatusFailedCommit})
			return
		}
		if commitCmd.ServiceID == kvs.id {
			if commitCmd.IsDuplicate {
				kvs.sendHTTPResponse(w, api.AppendResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				kvs.sendHTTPResponse(w, api.AppendResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					PrevValue:  commitCmd.ResultValue,
					Revision:   commitCmd.Revision,
				})
			}
		} else {
			kvs.sendHTTPResponse(w, api.AppendResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

func (kvs *KVService) handleGet(w http.ResponseWriter, req *http.Request) {
	gr := &api.GetRequest{}
	if err := readRequestJSON(req, gr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.logger.Debug("HTTP GET", "request", gr)

	cmd := Command{
		Kind:      CommandGet,
		Key:       gr.Key,
		ServiceID: kvs.id,
		ClientID:  gr.ClientID,
		RequestID: gr.RequestID,
	}
	result := kvs.rs.Submit(cmd)
	if result.Index < 0 {
		kvs.sendHTTPResponse(w, api.GetResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := kvs.createCommitSubscription(result.Index)

	select {
	case commitCmd, ok := <-sub:
		if !ok {
			// Subscription was cancelled because a snapshot covered this index.
			kvs.sendHTTPResponse(w, api.GetResponse{RespStatus: api.StatusFailedCommit})
			return
		}
		if commitCmd.ServiceID == kvs.id {
			if commitCmd.IsDuplicate && commitCmd.Kind != CommandGet {
				kvs.sendHTTPResponse(w, api.GetResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				kvs.sendHTTPResponse(w, api.GetResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					Value:      commitCmd.ResultValue,
					Revision:   commitCmd.Revision,
				})
			}
		} else {
			kvs.sendHTTPResponse(w, api.GetResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

func (kvs *KVService) handleCAS(w http.ResponseWriter, req *http.Request) {
	cr := &api.CASRequest{}
	if err := readRequestJSON(req, cr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.logger.Debug("HTTP CAS", "request", cr)

	cmd := Command{
		Kind:         CommandCAS,
		Key:          cr.Key,
		Value:        cr.Value,
		CompareValue: cr.CompareValue,
		ServiceID:    kvs.id,
		ClientID:     cr.ClientID,
		RequestID:    cr.RequestID,
	}
	result := kvs.rs.Submit(cmd)
	if result.Index < 0 {
		kvs.sendHTTPResponse(w, api.CASResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := kvs.createCommitSubscription(result.Index)

	select {
	case commitCmd, ok := <-sub:
		if !ok {
			// Subscription was cancelled because a snapshot covered this index.
			kvs.sendHTTPResponse(w, api.CASResponse{RespStatus: api.StatusFailedCommit})
			return
		}
		if commitCmd.ServiceID == kvs.id {
			if commitCmd.IsDuplicate {
				kvs.sendHTTPResponse(w, api.CASResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				kvs.sendHTTPResponse(w, api.CASResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					PrevValue:  commitCmd.ResultValue,
					Revision:   commitCmd.Revision,
				})
			}
		} else {
			kvs.sendHTTPResponse(w, api.CASResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

func (kvs *KVService) handleList(w http.ResponseWriter, req *http.Request) {
	lr := &api.ListRequest{}
	if err := readRequestJSON(req, lr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.logger.Debug("HTTP LIST", "request", lr)

	cmd := Command{
		Kind:      CommandList,
		Key:       lr.Prefix,
		ServiceID: kvs.id,
		ClientID:  lr.ClientID,
		RequestID: lr.RequestID,
	}
	result := kvs.rs.Submit(cmd)
	if result.Index < 0 {
		kvs.sendHTTPResponse(w, api.ListResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := kvs.createCommitSubscription(result.Index)

	select {
	case commitCmd, ok := <-sub:
		if !ok {
			kvs.sendHTTPResponse(w, api.ListResponse{RespStatus: api.StatusFailedCommit})
			return
		}
		if commitCmd.ServiceID == kvs.id {
			if commitCmd.IsDuplicate && commitCmd.Kind != CommandList {
				kvs.sendHTTPResponse(w, api.ListResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				kvs.sendHTTPResponse(w, api.ListResponse{
					RespStatus: api.StatusOK,
					Pairs:      commitCmd.ResultPairs,
					Revision:   commitCmd.Revision,
				})
			}
		} else {
			kvs.sendHTTPResponse(w, api.ListResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

// ---------------------------------------------------------------------------
// Admin / cluster management HTTP handlers
// ---------------------------------------------------------------------------

func (kvs *KVService) handleJoin(w http.ResponseWriter, req *http.Request) {
	jr := &api.JoinRequest{}
	if err := readRequestJSON(req, jr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.logger.Debug("HTTP JOIN", "request", jr)

	// Must be the leader to propose a config change.
	if !kvs.rs.IsLeader() {
		kvs.sendHTTPResponse(w, api.JoinResponse{RespStatus: api.StatusNotLeader})
		return
	}

	// Resolve the Raft address and establish a connection.
	raftAddr, err := net.ResolveTCPAddr("tcp", jr.RaftAddr)
	if err != nil {
		kvs.logger.Error("invalid raft address", "addr", jr.RaftAddr, "err", err)
		http.Error(w, "invalid raft address", http.StatusBadRequest)
		return
	}
	if err := kvs.rs.ConnectToPeer(jr.ID, raftAddr); err != nil {
		kvs.logger.Error("connect to new peer", "id", jr.ID, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Store the peer addresses so we can persist them when the config commits.
	kvs.Lock()
	kvs.pendingJoinAddrs[jr.ID] = peerInfo{RaftAddr: jr.RaftAddr, HTTPAddr: jr.HTTPAddr}
	kvs.Unlock()

	ok := kvs.rs.AddPeer(jr.ID, raftAddr)
	if !ok {
		kvs.Lock()
		delete(kvs.pendingJoinAddrs, jr.ID)
		kvs.Unlock()
		kvs.sendHTTPResponse(w, api.JoinResponse{RespStatus: api.StatusFailedCommit})
		return
	}

	kvs.sendHTTPResponse(w, api.JoinResponse{RespStatus: api.StatusOK})
}

func (kvs *KVService) handleLeave(w http.ResponseWriter, req *http.Request) {
	lr := &api.LeaveRequest{}
	if err := readRequestJSON(req, lr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.logger.Debug("HTTP LEAVE", "request", lr)

	if !kvs.rs.IsLeader() {
		kvs.sendHTTPResponse(w, api.LeaveResponse{RespStatus: api.StatusNotLeader})
		return
	}

	ok := kvs.rs.RemovePeer(lr.ID)
	if !ok {
		kvs.sendHTTPResponse(w, api.LeaveResponse{RespStatus: api.StatusFailedCommit})
		return
	}

	kvs.sendHTTPResponse(w, api.LeaveResponse{RespStatus: api.StatusOK})
}

func (kvs *KVService) handleStatus(w http.ResponseWriter, req *http.Request) {
	kvs.Lock()
	peerCount := len(kvs.peers)
	isLeader := kvs.rs.IsLeader()
	id := kvs.id
	kvs.Unlock()

	kvs.sendHTTPResponse(w, api.StatusResponse{
		RespStatus: api.StatusOK,
		ID:         id,
		IsLeader:   isLeader,
		PeerCount:  peerCount,
	})
}

func (kvs *KVService) handleMembers(w http.ResponseWriter, req *http.Request) {
	kvs.Lock()
	members := make([]api.PeerInfo, 0, len(kvs.peers)+1)
	members = append(members, api.PeerInfo{ID: kvs.id, HTTPAddr: kvs.httpAddr()})
	for id, pi := range kvs.peers {
		members = append(members, api.PeerInfo{ID: id, HTTPAddr: pi.HTTPAddr})
	}
	kvs.Unlock()

	kvs.sendHTTPResponse(w, api.MembersResponse{
		RespStatus: api.StatusOK,
		Members:    members,
	})
}

// httpAddr returns this node's own HTTP address as stored in the peer list,
// or empty string if unknown (no join has happened yet).
func (kvs *KVService) httpAddr() string {
	kvs.Lock()
	defer kvs.Unlock()
	if pi, ok := kvs.peers[kvs.id]; ok {
		return pi.HTTPAddr
	}
	return ""
}

// ---------------------------------------------------------------------------
// Peer list persistence in raft.Storage
// ---------------------------------------------------------------------------

// loadPeers restores the peer address book from durable storage.
func (kvs *KVService) loadPeers() {
	data, ok := kvs.storage.Get(peerStoreKey)
	if !ok {
		return
	}
	var stored []persistedPeer
	if err := json.Unmarshal(data, &stored); err != nil {
		kvs.logger.Error("failed to decode persisted peers", "err", err)
		return
	}
	kvs.Lock()
	defer kvs.Unlock()
	for _, p := range stored {
		if p.ID != kvs.id {
			kvs.peers[p.ID] = peerInfo{RaftAddr: p.RaftAddr, HTTPAddr: p.HTTPAddr}
		}
	}
	kvs.logger.Debug("loaded peers from storage", "peers", stored)
}

// savePeers persists the peer address book to durable storage.
func (kvs *KVService) savePeers() {
	kvs.Lock()
	stored := make([]persistedPeer, 0, len(kvs.peers)+1)
	for id, pi := range kvs.peers {
		stored = append(stored, persistedPeer{ID: id, RaftAddr: pi.RaftAddr, HTTPAddr: pi.HTTPAddr})
	}
	kvs.Unlock()

	data, err := json.Marshal(stored)
	if err != nil {
		kvs.logger.Error("failed to encode peers", "err", err)
		return
	}
	kvs.storage.Set(peerStoreKey, data)
}

// KnownPeers returns a copy of the peer address book for external callers
// (e.g. main.go) to reconnect to peers after restart.
func (kvs *KVService) KnownPeers() map[int]peerInfo {
	kvs.Lock()
	defer kvs.Unlock()
	out := make(map[int]peerInfo, len(kvs.peers))
	for id, pi := range kvs.peers {
		out[id] = pi
	}
	return out
}

// RaftServer exposes the underlying raft.Server for advanced operations
// in test harnesses and the main binary.
func (kvs *KVService) RaftServer() *raft.Server {
	return kvs.rs
}

// SetLocalHTTPAddr registers this node's own HTTP address in the peer book
// so that /members/ includes the local node and the address survives restarts.
func (kvs *KVService) SetLocalHTTPAddr(addr string) {
	kvs.Lock()
	kvs.peers[kvs.id] = peerInfo{HTTPAddr: addr}
	kvs.Unlock()
	kvs.savePeers()
}

// ---------------------------------------------------------------------------
// Snapshot encoding / decoding
// ---------------------------------------------------------------------------

// encodeSnapshot serialises the current KV state into a byte slice suitable
// for passing to raft.Server.InstallSnapshot.
// Must be called with kvs.Lock held.
func (kvs *KVService) encodeSnapshot() ([]byte, error) {
	snap := kvSnapshot{
		Store:                  kvs.ds.CopyAll(),
		LastRequestIDPerClient: make(map[int64]int64, len(kvs.lastRequestIDPerClient)),
		CurrentRevision:        kvs.currentRevision,
	}
	for k, v := range kvs.lastRequestIDPerClient {
		snap.LastRequestIDPerClient[k] = v
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snap); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeSnapshot deserialises a snapshot previously produced by encodeSnapshot.
func decodeSnapshot(data []byte) (kvSnapshot, error) {
	var snap kvSnapshot
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snap); err != nil {
		return kvSnapshot{}, err
	}
	return snap, nil
}

// maybeSnapshot takes a snapshot if enough entries have accumulated since the
// last one.  It is called after every committed entry, without kvs.Lock held.
// index and term are the Raft log position of the just-applied entry.
//
// The lock is held for the entire operation — including the InstallSnapshot
// call — so that a second entry committed in rapid succession cannot also pass
// the threshold check before the first call has updated lastSnapshotIndex
// inside the Raft layer, which would result in two overlapping snapshots.
// InstallSnapshot is non-blocking on the Raft side (it hands work off
// internally), so holding the KV lock here is safe.
func (kvs *KVService) maybeSnapshot(index, term int) {
	kvs.Lock()
	defer kvs.Unlock()

	if index-kvs.lastSnapshotIndex < snapshotThreshold {
		return
	}

	data, err := kvs.encodeSnapshot()
	if err != nil {
		kvs.logger.Error("failed to encode snapshot", "err", err)
		return
	}
	kvs.lastSnapshotIndex = index

	kvs.logger.Debug("installing snapshot", "index", index, "term", term)
	kvs.rs.InstallSnapshot(index, term, data)
}

// runUpdater is the main apply loop.  It drains both commitChan (normal log
// entries) and the Raft snapshot channel (InstallSnapshot from the leader).
//
// Snapshot processing is given priority over commit entry processing so that
// on restart the snapshot state (including lastRequestIDPerClient) is restored
// before any log entries are replayed.  Without this prioritization a race in
// the select loop can cause commitChan entries to be applied against an empty
// lastRequestIDPerClient, leading lastSnapshotIndex to be stuck at -1 and an
// incorrect snapshot to be taken.
func (kvs *KVService) runUpdater() {
	go func() {
		snapshotCh   := kvs.rs.SnapshotReady()
		snapshotDone := kvs.rs.SnapshotDone()

		handleSnapshot := func(snap raft.SnapshotEntry) {
			kvs.logger.Debug("restoring from snapshot", "index", snap.Index)

			decoded, err := decodeSnapshot(snap.Data)
			if err != nil {
				kvs.logger.Error("failed to decode snapshot", "err", err)
				return
			}

			kvs.Lock()
			kvs.ds.RestoreAll(decoded.Store)
			kvs.lastRequestIDPerClient = decoded.LastRequestIDPerClient
			kvs.currentRevision = decoded.CurrentRevision
			kvs.lastSnapshotIndex = snap.Index

			// Cancel any pending subscriptions for indices that are now
			// covered by the snapshot — those requests are gone.
			for idx, ch := range kvs.commitSubs {
				if idx <= snap.Index {
					close(ch)
					delete(kvs.commitSubs, idx)
				}
			}
			kvs.Unlock()
		}

		for {
			// Priority: drain any pending snapshot before processing commits.
			// Without this, a race on startup between SnapshotReady() and
			// commitChan can cause log entries to be applied before the snapshot
			// state is restored, corrupting lastRequestIDPerClient and
			// lastSnapshotIndex.
			select {
			case snap, ok := <-snapshotCh:
				if !ok {
					return
				}
				handleSnapshot(snap)
				continue
			default:
			}

			select {
			case snap, ok := <-snapshotCh:
				if !ok {
					return
				}
				handleSnapshot(snap)

			// ----------------------------------------------------------------
			// A normal committed log entry arrived.
			// ----------------------------------------------------------------
			case entry, ok := <-kvs.commitChan:
				if !ok {
					return
				}

				switch cmd := entry.Command.(type) {
				case Command:
					kvs.handleCommand(cmd, entry.Index, entry.Term)
				case raft.ConfigChangeEntry:
					kvs.handleCommittedConfigChange(cmd)
				}

			case <-snapshotDone:
				return
			}
		}
	}()
}

// handleCommand applies a committed Command entry to the state machine and
// notifies any waiting HTTP handler via the commit subscription.
func (kvs *KVService) handleCommand(cmd Command, index, term int) {
	kvs.Lock()
	lastReqID, exists := kvs.lastRequestIDPerClient[cmd.ClientID]
	if exists && lastReqID >= cmd.RequestID {
		kvs.logger.Debug("duplicate request", "requestID", cmd.RequestID, "clientID", cmd.ClientID)
		if cmd.Kind == CommandGet {
			cmd.ResultValue, cmd.ResultFound = kvs.ds.Get(cmd.Key)
		} else if cmd.Kind == CommandList {
			cmd.ResultPairs = kvs.ds.List(cmd.Key)
		}
		cmd.IsDuplicate = true
	} else {
		kvs.lastRequestIDPerClient[cmd.ClientID] = cmd.RequestID

		switch cmd.Kind {
		case CommandGet:
			cmd.ResultValue, cmd.ResultFound = kvs.ds.Get(cmd.Key)
		case CommandPut:
			cmd.ResultValue, cmd.ResultFound = kvs.ds.Put(cmd.Key, cmd.Value)
		case CommandAppend:
			cmd.ResultValue, cmd.ResultFound = kvs.ds.Append(cmd.Key, cmd.Value)
		case CommandCAS:
			cmd.ResultValue, cmd.ResultFound = kvs.ds.CAS(cmd.Key, cmd.CompareValue, cmd.Value)
		case CommandList:
			cmd.ResultPairs = kvs.ds.List(cmd.Key)
		default:
			kvs.Unlock()
			panic(fmt.Errorf("unexpected command %v", cmd))
		}
	}

	// Assign revision: increment on non-duplicate writes, snapshot current for reads
	switch cmd.Kind {
	case CommandPut, CommandAppend, CommandCAS:
		if !cmd.IsDuplicate {
			kvs.currentRevision++
		}
		cmd.Revision = kvs.currentRevision
	default:
		cmd.Revision = kvs.currentRevision
	}

	sub := kvs.popCommitSubscriptionLocked(index)
	kvs.Unlock()

	if sub != nil {
		sub <- cmd
		close(sub)
	}

	// Push watch events for non-duplicate mutations
	if !cmd.IsDuplicate {
		switch cmd.Kind {
		case CommandPut:
			kvs.pushWatchEvent(api.WatchEvent{
				Key: cmd.Key, Value: cmd.Value,
				Revision: cmd.Revision, Type: api.EventPut,
			})
		case CommandAppend:
			v, _ := kvs.ds.Get(cmd.Key)
			kvs.pushWatchEvent(api.WatchEvent{
				Key: cmd.Key, Value: v,
				Revision: cmd.Revision, Type: api.EventPut,
			})
		case CommandCAS:
			// Only emit an event if the compare matched and value was updated.
			// ResultFound indicates the key existed; compare ResultValue against
			// CompareValue to determine if the CAS actually changed the value.
			if cmd.ResultFound && cmd.ResultValue == cmd.CompareValue {
				v, _ := kvs.ds.Get(cmd.Key)
				kvs.pushWatchEvent(api.WatchEvent{
					Key: cmd.Key, Value: v,
					Revision: cmd.Revision, Type: api.EventPut,
				})
			}
		}
	}

	if kvs.lastSnapshotIndex >= 0 {
		kvs.maybeSnapshot(index, term)
	}
}

// handleCommittedConfigChange updates the peer address book when a Raft
// membership change (AddPeer / RemovePeer) commits through the log.
func (kvs *KVService) handleCommittedConfigChange(cc raft.ConfigChangeEntry) {
	kvs.Lock()
	defer kvs.Unlock()

	switch cc.Type {
	case raft.AddNode:
		if pi, ok := kvs.pendingJoinAddrs[cc.NodeId]; ok {
			kvs.peers[cc.NodeId] = pi
			delete(kvs.pendingJoinAddrs, cc.NodeId)
			kvs.logger.Debug("peer added", "id", cc.NodeId, "addr", pi.HTTPAddr)
		}
		if cc.NodeId != kvs.id {
			kvs.peerIDs = append(kvs.peerIDs, cc.NodeId)
		}

	case raft.RemoveNode:
		delete(kvs.peers, cc.NodeId)
		newIDs := kvs.peerIDs[:0:0]
		for _, id := range kvs.peerIDs {
			if id != cc.NodeId {
				newIDs = append(newIDs, id)
			}
		}
		kvs.peerIDs = newIDs
		kvs.logger.Debug("peer removed", "id", cc.NodeId)
	}
	go kvs.savePeers()
}

// PeerIDs returns a copy of the current Raft peer IDs.
func (kvs *KVService) PeerIDs() []int {
	kvs.Lock()
	defer kvs.Unlock()
	return append([]int(nil), kvs.peerIDs...)
}

func (kvs *KVService) createCommitSubscription(logIndex int) chan Command {
	kvs.Lock()
	defer kvs.Unlock()

	if _, exists := kvs.commitSubs[logIndex]; exists {
		panic(fmt.Sprintf("duplicate commit subscription for logIndex=%d", logIndex))
	}

	ch := make(chan Command, 1)
	kvs.commitSubs[logIndex] = ch
	return ch
}

// popCommitSubscriptionLocked removes and returns the subscription channel for
// logIndex.  Must be called with kvs.Lock held.
func (kvs *KVService) popCommitSubscriptionLocked(logIndex int) chan Command {
	ch := kvs.commitSubs[logIndex]
	delete(kvs.commitSubs, logIndex)
	return ch
}

// The following functions exist for testing purposes, to simulate faults.
func (kvs *KVService) ConnectToRaftPeer(peerId int, addr net.Addr) error {
	return kvs.rs.ConnectToPeer(peerId, addr)
}

func (kvs *KVService) DisconnectFromAllRaftPeers() {
	kvs.rs.DisconnectAll()
}

func (kvs *KVService) DisconnectFromRaftPeer(peerId int) error {
	return kvs.rs.DisconnectPeer(peerId)
}

func (kvs *KVService) GetRaftListenAddr() net.Addr {
	return kvs.rs.GetListenAddr()
}
