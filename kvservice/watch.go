package kvservice

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/MHS-20/Zodiac/api"
)

// ---------------------------------------------------------------------------
// Event ring buffer (in-memory, not snapshotted)
// ---------------------------------------------------------------------------

const eventBufferSize = 1000

type eventBuffer struct {
	mu     sync.Mutex
	events []api.WatchEvent
	start  int // oldest valid index in the ring
	count  int // number of events currently stored
	size   int
}

func newEventBuffer(size int) *eventBuffer {
	return &eventBuffer{
		events: make([]api.WatchEvent, 0, size),
		size:   size,
	}
}

func (eb *eventBuffer) Push(e api.WatchEvent) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if eb.count == eb.size {
		eb.events[eb.start] = e
		eb.start = (eb.start + 1) % eb.size
	} else {
		idx := (eb.start + eb.count) % eb.size
		if idx >= len(eb.events) {
			eb.events = append(eb.events, e)
		} else {
			eb.events[idx] = e
		}
		eb.count++
	}
}

func (eb *eventBuffer) Replay(since int64) []api.WatchEvent {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	var result []api.WatchEvent
	for i := 0; i < eb.count; i++ {
		idx := (eb.start + i) % eb.size
		e := eb.events[idx]
		if e.Revision > since {
			result = append(result, e)
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Watch subscription
// ---------------------------------------------------------------------------

type watchSub struct {
	prefix string
	ch     chan api.WatchEvent
}

func newWatchSub(prefix string, bufsize int) *watchSub {
	return &watchSub{
		prefix: prefix,
		ch:     make(chan api.WatchEvent, bufsize),
	}
}

// ---------------------------------------------------------------------------
// SSE handler (GET /watch/)
// ---------------------------------------------------------------------------

func (kvs *KVService) handleWatch(w http.ResponseWriter, req *http.Request) {
	prefix := req.URL.Query().Get("prefix")
	sinceStr := req.URL.Query().Get("since")
	lastEventID := req.Header.Get("Last-Event-ID")

	if lastEventID != "" {
		if rev, err := strconv.ParseInt(lastEventID, 10, 64); err == nil && rev > 0 {
			sinceStr = lastEventID
		}
	}

	var since int64
	if sinceStr != "" {
		var err error
		since, err = strconv.ParseInt(sinceStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid since parameter", http.StatusBadRequest)
			return
		}
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	subID := kvs.subscribeWatch(prefix)
	defer kvs.unsubscribeWatch(subID)

	events := kvs.eventBuf.Replay(since)
	for _, e := range events {
		writeSSE(w, e)
	}
	flusher.Flush()

	ch := kvs.getWatchChan(subID)

	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return
			}
			writeSSE(w, e)
			flusher.Flush()
		case <-req.Context().Done():
			return
		}
	}
}

func writeSSE(w io.Writer, e api.WatchEvent) {
	data, _ := json.Marshal(e)
	fmt.Fprintf(w, "event: %s\n", api.WatchEventName[e.Type])
	fmt.Fprintf(w, "id: %d\n", e.Revision)
	fmt.Fprintf(w, "data: %s\n\n", data)
}

// ---------------------------------------------------------------------------
// Subscription manager
// ---------------------------------------------------------------------------

func (kvs *KVService) subscribeWatch(prefix string) int64 {
	kvs.watchMu.Lock()
	defer kvs.watchMu.Unlock()

	kvs.watchSeq++
	id := kvs.watchSeq
	kvs.watchSubs[id] = newWatchSub(prefix, 64)
	return id
}

func (kvs *KVService) unsubscribeWatch(id int64) {
	kvs.watchMu.Lock()
	defer kvs.watchMu.Unlock()

	if sub, ok := kvs.watchSubs[id]; ok {
		close(sub.ch)
		delete(kvs.watchSubs, id)
	}
}

func (kvs *KVService) getWatchChan(id int64) <-chan api.WatchEvent {
	kvs.watchMu.Lock()
	defer kvs.watchMu.Unlock()

	if sub, ok := kvs.watchSubs[id]; ok {
		return sub.ch
	}
	ch := make(chan api.WatchEvent)
	close(ch)
	return ch
}

// pushWatchEvent pushes an event into the ring buffer and delivers it
// to all subscribers whose prefix matches the event key.
func (kvs *KVService) pushWatchEvent(e api.WatchEvent) {
	kvs.eventBuf.Push(e)

	kvs.watchMu.Lock()
	for _, sub := range kvs.watchSubs {
		if sub.prefix == "" || strings.HasPrefix(e.Key, sub.prefix) {
			select {
			case sub.ch <- e:
			default:
			}
		}
	}
	kvs.watchMu.Unlock()
}