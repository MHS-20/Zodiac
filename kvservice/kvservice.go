package kvservice

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/MHS-20/Raft/raft"
	"github.com/MHS-20/Zodiac/api"
)

const DebugKV = 1

type KVService struct {
	sync.Mutex
	id int
	rs *raft.Server

	// commitChan is the commit channel passed to the Raft server; when commands
	// are committed, they're sent on this channel.
	commitChan chan raft.CommitEntry

	// commitSubs are the commit subscriptions currently active in this service.
	commitSubs map[int]chan Command

	ds  *DataStore
	srv *http.Server

	// for testing and debugging.
	httpResponsesEnabled bool
}

func New(id int, peerIds []int, storage raft.Storage, readyChan <-chan any) *KVService {
	gob.Register(Command{})
	commitChan := make(chan raft.CommitEntry)

	rs := raft.NewServer(id, peerIds, storage, readyChan, commitChan)
	rs.Serve()
	kvs := &KVService{
		id:                   id,
		rs:                   rs,
		commitChan:           commitChan,
		ds:                   NewDataStore(),
		commitSubs:           make(map[int]chan Command),
		httpResponsesEnabled: true,
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
	mux.HandleFunc("POST /cas/", kvs.handleCAS)

	kvs.srv = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		kvs.kvlog("serving HTTP on %s", kvs.srv.Addr)
		if err := kvs.srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
		kvs.srv = nil
	}()
}

// Note: DisconnectFromRaftPeers on all peers in the cluster should be done
// before Shutdown is called.
func (kvs *KVService) Shutdown() error {
	kvs.kvlog("shutting down Raft server")
	kvs.rs.Shutdown()
	kvs.kvlog("closing commitChan")
	close(kvs.commitChan)

	if kvs.srv != nil {
		kvs.kvlog("shutting down HTTP server")
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		kvs.srv.Shutdown(ctx)
		kvs.kvlog("HTTP shutdown complete")
		return nil
	}

	return nil
}

// For testing and debugging purposes, this method can be called with false;
// then, the service will not respond to clients over HTTP.
func (kvs *KVService) ToggleHTTPResponsesEnabled(enable bool) {
	kvs.httpResponsesEnabled = enable
}

func (kvs *KVService) sendHTTPResponse(w http.ResponseWriter, v any) {
	if kvs.httpResponsesEnabled {
		renderJSON(w, v)
	}
}

func (kvs *KVService) handlePut(w http.ResponseWriter, req *http.Request) {
	pr := &api.PutRequest{}
	if err := readRequestJSON(req, pr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.kvlog("HTTP PUT %v", pr)

	// Submit a command into the Raft server; this is the state change in the
	// replicated state machine built on top of the Raft log.
	cmd := Command{
		Kind:  CommandPut,
		Key:   pr.Key,
		Value: pr.Value,
		Id:    kvs.id,
	}
	logIndex := kvs.rs.Submit(cmd)
	// If we're not the Raft leader, send an appropriate status
	if logIndex < 0 {
		kvs.sendHTTPResponse(w, api.PutResponse{RespStatus: api.StatusNotLeader})
		return
	}

	// Subscribe for a commit update for our log index
	sub := kvs.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.Id == kvs.id {
			kvs.sendHTTPResponse(w, api.PutResponse{
				RespStatus: api.StatusOK,
				KeyFound:   commitCmd.ResultFound,
				PrevValue:  commitCmd.ResultValue,
			})
		} else {
			// lost leadership
			kvs.sendHTTPResponse(w, api.PutResponse{RespStatus: api.StatusFailedCommit})
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
	kvs.kvlog("HTTP GET %v", gr)

	cmd := Command{
		Kind: CommandGet,
		Key:  gr.Key,
		Id:   kvs.id,
	}
	logIndex := kvs.rs.Submit(cmd)
	if logIndex < 0 {
		kvs.sendHTTPResponse(w, api.GetResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := kvs.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.Id == kvs.id {
			kvs.sendHTTPResponse(w, api.GetResponse{
				RespStatus: api.StatusOK,
				KeyFound:   commitCmd.ResultFound,
				Value:      commitCmd.ResultValue,
			})
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
	kvs.kvlog("HTTP CAS %v", cr)

	cmd := Command{
		Kind:         CommandCAS,
		Key:          cr.Key,
		Value:        cr.Value,
		CompareValue: cr.CompareValue,
		Id:           kvs.id,
	}
	logIndex := kvs.rs.Submit(cmd)
	if logIndex < 0 {
		kvs.sendHTTPResponse(w, api.PutResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := kvs.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.Id == kvs.id {
			kvs.sendHTTPResponse(w, api.CASResponse{
				RespStatus: api.StatusOK,
				KeyFound:   commitCmd.ResultFound,
				PrevValue:  commitCmd.ResultValue,
			})
		} else {
			kvs.sendHTTPResponse(w, api.CASResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

// goroutine that reads the commit channel from Raft and updates the data store
// It also notifies subscribers
func (kvs *KVService) runUpdater() {
	go func() {
		for entry := range kvs.commitChan {
			cmd := entry.Command.(Command)

			switch cmd.Kind {
			case CommandGet:
				cmd.ResultValue, cmd.ResultFound = kvs.ds.Get(cmd.Key)
			case CommandPut:
				cmd.ResultValue, cmd.ResultFound = kvs.ds.Put(cmd.Key, cmd.Value)
			case CommandCAS:
				cmd.ResultValue, cmd.ResultFound = kvs.ds.CAS(cmd.Key, cmd.CompareValue, cmd.Value)
			default:
				panic(fmt.Errorf("unexpected command %v", cmd))
			}

			// Forward this entry to the subscriber interested in its index, and
			// close the subscription - it's single-use.
			if sub := kvs.popCommitSubscription(entry.Index); sub != nil {
				sub <- cmd
				close(sub)
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

func (kvs *KVService) popCommitSubscription(logIndex int) chan Command {
	kvs.Lock()
	defer kvs.Unlock()

	ch := kvs.commitSubs[logIndex]
	delete(kvs.commitSubs, logIndex)
	return ch
}

func (kvs *KVService) kvlog(format string, args ...any) {
	if DebugKV > 0 {
		format = fmt.Sprintf("[kv %d] ", kvs.id) + format
		log.Printf(format, args...)
	}
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
