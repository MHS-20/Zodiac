package kvclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/MHS-20/Zodiac/api"
)

const DebugClient = 1

type KVClient struct {
	addrs []string

	// index of the service we assume is the current leader
	assumedLeader int
	clientID      int64

	// each client manages its own requestID, and increments it monotonically and
	// atomically each time the user asks to send a new request.
	requestID atomic.Int64
}

func New(serviceAddrs []string) *KVClient {
	return &KVClient{
		addrs:         serviceAddrs,
		assumedLeader: 0,
		clientID:      clientCount.Add(1),
	}
}

var clientCount atomic.Int64

func (c *KVClient) Put(ctx context.Context, key string, value string) (string, bool, error) {
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
	return putResp.PrevValue, putResp.KeyFound, err
}

func (c *KVClient) Append(ctx context.Context, key string, value string) (string, bool, error) {
	appendReq := api.AppendRequest{
		Key:       key,
		Value:     value,
		ClientID:  c.clientID,
		RequestID: c.requestID.Add(1),
	}
	var appendResp api.AppendResponse
	err := c.send(ctx, "append", appendReq, &appendResp)
	return appendResp.PrevValue, appendResp.KeyFound, err
}

func (c *KVClient) Get(ctx context.Context, key string) (string, bool, error) {
	getReq := api.GetRequest{
		Key:       key,
		ClientID:  c.clientID,
		RequestID: c.requestID.Add(1),
	}
	var getResp api.GetResponse
	err := c.send(ctx, "get", getReq, &getResp)
	return getResp.Value, getResp.KeyFound, err
}

func (c *KVClient) CAS(ctx context.Context, key string, compare string, value string) (string, bool, error) {
	casReq := api.CASRequest{
		Key:          key,
		CompareValue: compare,
		Value:        value,
		ClientID:     c.clientID,
		RequestID:    c.requestID.Add(1),
	}
	var casResp api.CASResponse
	err := c.send(ctx, "cas", casReq, &casResp)
	return casResp.PrevValue, casResp.KeyFound, err
}

func (c *KVClient) send(ctx context.Context, route string, req any, resp api.Response) error {
	// This loop rotates through the list of service addresses until we get
	// a response that indicates we've found the leader of the cluster
FindLeader:
	for {
		retryCtx, retryCtxCancel := context.WithTimeout(ctx, 50*time.Millisecond)
		path := fmt.Sprintf("http://%s/%s/", c.addrs[c.assumedLeader], route)

		c.clientlog("sending %#v to %v", req, path)
		if err := sendJSONRequest(retryCtx, path, req, resp); err != nil {
			if contextDone(ctx) {
				c.clientlog("parent context done; bailing out")
				retryCtxCancel()
				return err
			} else if contextDeadlineExceeded(retryCtx) {
				// retry a different service.
				c.clientlog("timed out: will try next address")
				c.assumedLeader = (c.assumedLeader + 1) % len(c.addrs)
				retryCtxCancel()
				continue FindLeader
			}
			retryCtxCancel()
			return err
		}
		c.clientlog("received response %#v", resp)

		// response received
		switch resp.Status() {
		case api.StatusNotLeader:
			c.clientlog("not leader: will try next address")
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

func (c *KVClient) clientlog(format string, args ...any) {
	if DebugClient > 0 {
		clientName := fmt.Sprintf("[client%03d]", c.clientID)
		format = clientName + " " + format
		log.Printf(format, args...)
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
