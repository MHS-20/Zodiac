package kvservice

import (
	"bytes"
	"context"
	"encoding/gob"
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
const snapshotThreshold = 100

// kvSnapshot is the serialisable state that gets stored in a Raft snapshot.
type kvSnapshot struct {
	Store                  map[string]string
	LastRequestIDPerClient map[int64]int64
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
	delayNextHTTPResponse  atomic.Bool

	// lastSnapshotIndex tracks the log index of the most recent snapshot so we
	// know how many entries have accumulated since the last compaction.
	lastSnapshotIndex int
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
	}

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
				})
			}
		} else {
			kvs.sendHTTPResponse(w, api.CASResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

// encodeSnapshot serialises the current KV state into a byte slice suitable
// for passing to raft.Server.InstallSnapshot.
// Must be called with kvs.Lock held.
func (kvs *KVService) encodeSnapshot() ([]byte, error) {
	snap := kvSnapshot{
		Store:                  kvs.ds.CopyAll(),
		LastRequestIDPerClient: make(map[int64]int64, len(kvs.lastRequestIDPerClient)),
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
				cmd := entry.Command.(Command)

				// Duplicate command detection.
				kvs.Lock()
				lastReqID, exists := kvs.lastRequestIDPerClient[cmd.ClientID]
				if exists && lastReqID >= cmd.RequestID {
					kvs.logger.Debug("duplicate request", "requestID", cmd.RequestID, "clientID", cmd.ClientID)
					// Mark as duplicate but still execute Get commands —
					// they are read-only and the caller needs the value.
					if cmd.Kind == CommandGet {
						cmd.ResultValue, cmd.ResultFound = kvs.ds.Get(cmd.Key)
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
					default:
						kvs.Unlock()
						panic(fmt.Errorf("unexpected command %v", cmd))
					}
				}

				sub := kvs.popCommitSubscriptionLocked(entry.Index)
				kvs.Unlock()

				if sub != nil {
					sub <- cmd
					close(sub)
				}

				// Trigger a snapshot if we've accumulated enough entries.
				// Only do this after the initial snapshot has been restored,
				// otherwise lastSnapshotIndex is -1 and an incorrect snapshot
				// would be taken with an empty lastRequestIDPerClient.
				if kvs.lastSnapshotIndex >= 0 {
					kvs.maybeSnapshot(entry.Index, entry.Term)
				}

			case <-snapshotDone:
				return
			}
		}
	}()
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
