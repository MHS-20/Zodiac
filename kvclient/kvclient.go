package kvclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/MHS-20/Zodiac/api"
)

type KVClient struct {
	addrs []string

	// index of the service we assume is the current leader
	assumedLeader int
	clientID      int64
	logger        *slog.Logger

	// each client manages its own requestID, and increments it monotonically and
	// atomically each time the user asks to send a new request.
	requestID atomic.Int64
}

func New(serviceAddrs []string) *KVClient {
	id := clientCount.Add(1)
	return &KVClient{
		addrs:         serviceAddrs,
		assumedLeader: 0,
		clientID:      id,
		logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})).With("component", "client", "clientID", id),
	}
}

var clientCount atomic.Int64

func (c *KVClient) Put(ctx context.Context, key string, value string) (string, bool, int64, error) {
	// The unique ID within each request helps the service de-duplicate requests that may
	// arrive multiple times due to network issues and client retries.
	putReq := api.PutRequest{
		Key:       key,
		Value:     value,
		ClientID:  c.clientID,
		RequestID: c.requestID.Add(1),
	}
	var putResp api.PutResponse
	err := c.send(ctx, "put", putReq, &putResp)
	return putResp.PrevValue, putResp.KeyFound, putResp.Revision, err
}

func (c *KVClient) Append(ctx context.Context, key string, value string) (string, bool, int64, error) {
	appendReq := api.AppendRequest{
		Key:       key,
		Value:     value,
		ClientID:  c.clientID,
		RequestID: c.requestID.Add(1),
	}
	var appendResp api.AppendResponse
	err := c.send(ctx, "append", appendReq, &appendResp)
	return appendResp.PrevValue, appendResp.KeyFound, appendResp.Revision, err
}

func (c *KVClient) Get(ctx context.Context, key string) (string, bool, int64, error) {
	getReq := api.GetRequest{
		Key:       key,
		ClientID:  c.clientID,
		RequestID: c.requestID.Add(1),
	}
	var getResp api.GetResponse
	err := c.send(ctx, "get", getReq, &getResp)
	return getResp.Value, getResp.KeyFound, getResp.Revision, err
}

func (c *KVClient) List(ctx context.Context, prefix string) (map[string]string, int64, error) {
	listReq := api.ListRequest{
		Prefix:    prefix,
		ClientID:  c.clientID,
		RequestID: c.requestID.Add(1),
	}
	var listResp api.ListResponse
	err := c.send(ctx, "list", listReq, &listResp)
	return listResp.Pairs, listResp.Revision, err
}

func (c *KVClient) CAS(ctx context.Context, key string, compare string, value string) (string, bool, int64, error) {
	casReq := api.CASRequest{
		Key:          key,
		CompareValue: compare,
		Value:        value,
		ClientID:     c.clientID,
		RequestID:    c.requestID.Add(1),
	}
	var casResp api.CASResponse
	err := c.send(ctx, "cas", casReq, &casResp)
	return casResp.PrevValue, casResp.KeyFound, casResp.Revision, err
}

func (c *KVClient) send(ctx context.Context, route string, req any, resp api.Response) error {
	// This loop rotates through the list of service addresses until we get
	// a response that indicates we've found the leader of the cluster
FindLeader:
	for {
		retryCtx, retryCtxCancel := context.WithTimeout(ctx, 50*time.Millisecond)
		path := fmt.Sprintf("http://%s/%s/", c.addrs[c.assumedLeader], route)

		c.logger.Debug("sending request", "path", path, "request", fmt.Sprintf("%#v", req))
		if err := sendJSONRequest(retryCtx, path, req, resp); err != nil {
			if contextDone(ctx) {
				c.logger.Debug("parent context done; bailing out")
				retryCtxCancel()
				return err
			} else if contextDeadlineExceeded(retryCtx) {
				// retry a different service.
				c.logger.Debug("timed out; will try next address")
				c.assumedLeader = (c.assumedLeader + 1) % len(c.addrs)
				retryCtxCancel()
				continue FindLeader
			}
			retryCtxCancel()
			return err
		}
		c.logger.Debug("received response", "response", fmt.Sprintf("%#v", resp))

		// response received
		switch resp.Status() {
		case api.StatusNotLeader:
			c.logger.Debug("not leader; will try next address")
			c.assumedLeader = (c.assumedLeader + 1) % len(c.addrs)
			retryCtxCancel()
			continue FindLeader
		case api.StatusOK:
			retryCtxCancel()
			return nil
		case api.StatusFailedCommit:
			retryCtxCancel()
			return fmt.Errorf("commit failed; please retry")
		case api.StatusDuplicateRequest:
			retryCtxCancel()
			return fmt.Errorf("this request was already completed")
		default:
			panic("unreachable")
		}
	}
}

func sendJSONRequest(ctx context.Context, path string, reqData any, respData any) error {
	body := new(bytes.Buffer)
	enc := json.NewEncoder(body)
	if err := enc.Encode(reqData); err != nil {
		return fmt.Errorf("JSON-encoding request data: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, path, body)
	if err != nil {
		return fmt.Errorf("creating HTTP request: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(respData); err != nil {
		return fmt.Errorf("JSON-decoding response data: %w", err)
	}
	return nil
}

func contextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return false
}

func contextDeadlineExceeded(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return true
		}
	default:
	}
	return false
}

// ---------------------------------------------------------------------------
// Dynamic cluster discovery
// ---------------------------------------------------------------------------

// DiscoverMembers queries any reachable node's /members/ endpoint and returns
// the full list of cluster members.  It tries each seed address in order.
func DiscoverMembers(ctx context.Context, seedAddrs []string) ([]api.PeerInfo, error) {
	for i, addr := range seedAddrs {
		discoverCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		path := fmt.Sprintf("http://%s/members/", addr)
		req, err := http.NewRequestWithContext(discoverCtx, http.MethodGet, path, nil)
		if err != nil {
			cancel()
			continue
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			if i == len(seedAddrs)-1 {
				return nil, fmt.Errorf("discovery failed: %w", err)
			}
			continue
		}
		var membersResp api.MembersResponse
		if err := json.NewDecoder(resp.Body).Decode(&membersResp); err != nil {
			resp.Body.Close()
			cancel()
			continue
		}
		resp.Body.Close()
		cancel()
		if membersResp.RespStatus == api.StatusOK {
			return membersResp.Members, nil
		}
	}
	return nil, fmt.Errorf("discovery failed: no reachable seed nodes")
}

// NewWithDiscovery creates a client with a seed set of addresses, discovers
// the full cluster membership via /members/, and populates the address list
// from the response.  If discovery fails, the seed addresses are used as-is.
func NewWithDiscovery(ctx context.Context, seedAddrs []string) *KVClient {
	members, err := DiscoverMembers(ctx, seedAddrs)
	if err != nil {
		return New(seedAddrs)
	}
	addrs := make([]string, 0, len(members))
	for _, m := range members {
		if m.HTTPAddr != "" {
			addrs = append(addrs, m.HTTPAddr)
		}
	}
	if len(addrs) == 0 {
		return New(seedAddrs)
	}
	return New(addrs)
}

// Watch subscribes to an SSE event stream for keys matching the given prefix.
// If since > 0, events after that revision are replayed from the ring buffer.
// Returns a channel that receives WatchEvents until the context is cancelled
// or the connection drops.
func (c *KVClient) Watch(ctx context.Context, prefix string, since int64) (<-chan api.WatchEvent, error) {
	path := fmt.Sprintf("http://%s/watch/?prefix=%s&since=%d", c.addrs[c.assumedLeader], prefix, since)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("creating watch request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("watch request failed: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("watch request returned status %d", resp.StatusCode)
	}

	ch := make(chan api.WatchEvent, 64)
	go c.readSSE(ctx, resp.Body, ch)
	return ch, nil
}

func (c *KVClient) readSSE(ctx context.Context, body io.ReadCloser, ch chan<- api.WatchEvent) {
	defer body.Close()
	defer close(ch)

	scanner := bufio.NewScanner(body)
	var eventType string
	var eventID string
	var eventData string

	flushEvent := func() {
		if eventType == "" || eventData == "" {
			return
		}
		evType, ok := api.WatchEventTypeFromName[eventType]
		if !ok {
			return
		}
		var ev api.WatchEvent
		if err := json.Unmarshal([]byte(eventData), &ev); err != nil {
			return
		}
		ev.Type = evType
		if rev, err := strconv.ParseInt(eventID, 10, 64); err == nil && rev > 0 {
			ev.Revision = rev
		}
		select {
		case ch <- ev:
		case <-ctx.Done():
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			flushEvent()
			eventType = ""
			eventID = ""
			eventData = ""
			continue
		}
		if strings.HasPrefix(line, "event: ") {
			eventType = strings.TrimPrefix(line, "event: ")
		} else if strings.HasPrefix(line, "id: ") {
			eventID = strings.TrimPrefix(line, "id: ")
		} else if strings.HasPrefix(line, "data: ") {
			eventData = strings.TrimPrefix(line, "data: ")
		}
	}
	// Flush any partial event at EOF
	flushEvent()
}

// RefreshMembers updates the client's address list by querying /members/ on
// the currently assumed leader (or the first known address if no leader is
// assumed).  Returns the number of addresses discovered.
func (c *KVClient) RefreshMembers(ctx context.Context) (int, error) {
	addr := c.addrs[c.assumedLeader]
	members, err := DiscoverMembers(ctx, []string{addr})
	if err != nil {
		return 0, err
	}
	newAddrs := make([]string, 0, len(members))
	for _, m := range members {
		if m.HTTPAddr != "" {
			newAddrs = append(newAddrs, m.HTTPAddr)
		}
	}
	if len(newAddrs) > 0 {
		c.addrs = newAddrs
		if c.assumedLeader >= len(c.addrs) {
			c.assumedLeader = 0
		}
	}
	return len(newAddrs), nil
}
