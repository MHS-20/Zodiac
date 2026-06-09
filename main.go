package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MHS-20/Raft/raft"
	"github.com/MHS-20/Zodiac/api"
	"github.com/MHS-20/Zodiac/kvservice"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: zodiac <config.json>")
	}

	cfg, err := loadConfig(os.Args[1])
	if err != nil {
		log.Fatalf("Config error: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With("node", cfg.NodeID)

	storage, err := raft.NewFileStorage(cfg.DataDir)
	if err != nil {
		logger.Error("storage init", "err", err)
		os.Exit(1)
	}

	peerIDs := cfg.PeerIDs()

	ready := make(chan any)

	kvs := kvservice.New(cfg.NodeID, peerIDs, storage, ready)

	knownPeers := kvs.KnownPeers()
	connected := make(map[int]bool)
	connect := func(id int, raftAddrStr string) {
		if id == cfg.NodeID || connected[id] {
			return
		}
		raftAddr, err := net.ResolveTCPAddr("tcp", raftAddrStr)
		if err != nil {
			logger.Warn("resolve peer address", "peer", id, "addr", raftAddrStr, "err", err)
			return
		}
		if err := kvs.ConnectToRaftPeer(id, raftAddr); err != nil {
			logger.Warn("connect to peer", "peer", id, "err", err)
			return
		}
		connected[id] = true
	}

	for id, pi := range knownPeers {
		if pi.RaftAddr != "" {
			connect(id, pi.RaftAddr)
		}
	}
	for _, p := range cfg.InitialCluster {
		connect(p.ID, p.RaftAddr)
	}

	close(ready)

	httpAddr := cfg.HTTPAddr()
	kvs.ServeHTTP(cfg.HTTPPort)

	kvs.SetLocalHTTPAddr(httpAddr)

	logger.Info("node started", "http", httpAddr, "raft", kvs.GetRaftListenAddr().String())

	if !cfg.isInitialMember() {
		joinCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := joinCluster(joinCtx, kvs, cfg)
		cancel()
		if err != nil {
			logger.Warn("join cluster", "err", err)
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("shutting down", "signal", sig)
	kvs.Shutdown()
}

// joinCluster discovers a cluster member via the initial seeds and sends a
// /join/ request to the leader.
func joinCluster(ctx context.Context, kvs *kvservice.KVService, cfg *Config) error {
	seeds := make([]string, 0, len(cfg.InitialCluster))
	for _, p := range cfg.InitialCluster {
		seeds = append(seeds, p.HTTPAddr)
	}
	if len(seeds) == 0 {
		return nil
	}

	raftAddr := kvs.GetRaftListenAddr().String()
	httpAddr := cfg.HTTPAddr()

	for _, seed := range seeds {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		leader, err := findLeader(ctx, seed, seeds)
		if err != nil {
			continue
		}

		if err := sendJoin(ctx, leader, cfg.NodeID, raftAddr, httpAddr); err != nil {
			continue
		}
		return nil
	}
	return ctx.Err()
}

// findLeader discovers the current Raft leader by probing the known nodes.
// If the seed node is not the leader, it fetches /members/ and probes each.
func findLeader(ctx context.Context, seedAddr string, allSeeds []string) (string, error) {
	probe := func(addr string) (bool, error) {
		probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		path := fmt.Sprintf("http://%s/status/", addr)
		req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, path, nil)
		if err != nil {
			return false, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()
		var status api.StatusResponse
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			return false, err
		}
		return status.IsLeader && status.RespStatus == api.StatusOK, nil
	}

	isLeader, err := probe(seedAddr)
	if err == nil && isLeader {
		return seedAddr, nil
	}

	// Fetch members from the seed.
	members, err := fetchMembers(ctx, seedAddr)
	if err != nil {
		// Fall back to probing all seeds.
		for _, addr := range allSeeds {
			isLeader, err := probe(addr)
			if err == nil && isLeader {
				return addr, nil
			}
		}
		return "", fmt.Errorf("leader not found")
	}

	for _, m := range members {
		if m.HTTPAddr == "" {
			continue
		}
		isLeader, err := probe(m.HTTPAddr)
		if err == nil && isLeader {
			return m.HTTPAddr, nil
		}
	}
	return "", fmt.Errorf("leader not found among members")
}

// fetchMembers calls GET /members/ on the given address.
func fetchMembers(ctx context.Context, addr string) ([]api.PeerInfo, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	path := fmt.Sprintf("http://%s/members/", addr)
	req, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var mr api.MembersResponse
	if err := json.NewDecoder(resp.Body).Decode(&mr); err != nil {
		return nil, err
	}
	if mr.RespStatus != api.StatusOK {
		return nil, fmt.Errorf("members response status %v", mr.RespStatus)
	}
	return mr.Members, nil
}

// sendJoin POSTs a JoinRequest to the leader's /join/ endpoint.
func sendJoin(ctx context.Context, leaderAddr string, nodeID int, raftAddr, httpAddr string) error {
	jr := api.JoinRequest{
		ID:       nodeID,
		RaftAddr: raftAddr,
		HTTPAddr: httpAddr,
	}
	body := new(bytes.Buffer)
	if err := json.NewEncoder(body).Encode(jr); err != nil {
		return fmt.Errorf("encode join request: %w", err)
	}

	joinCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	path := fmt.Sprintf("http://%s/join/", leaderAddr)
	req, err := http.NewRequestWithContext(joinCtx, http.MethodPost, path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var joinResp api.JoinResponse
	if err := json.NewDecoder(resp.Body).Decode(&joinResp); err != nil {
		return err
	}
	if joinResp.RespStatus != api.StatusOK {
		return fmt.Errorf("join rejected: %v", joinResp.RespStatus)
	}
	return nil
}
