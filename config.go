package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config describes a single node's cluster configuration.
type Config struct {
	NodeID         int          `json:"node_id"`
	HTTPPort       int          `json:"http_port"`
	DataDir        string       `json:"data_dir"`
	InitialCluster []PeerConfig `json:"initial_cluster"`
}

// PeerConfig describes one member in the initial cluster.
type PeerConfig struct {
	ID       int    `json:"id"`
	HTTPAddr string `json:"http_addr"`
	RaftAddr string `json:"raft_addr"`
}

// loadConfig reads and parses a JSON config file.
func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	if cfg.NodeID < 0 {
		return nil, fmt.Errorf("node_id must be >= 0")
	}
	if cfg.HTTPPort <= 0 {
		return nil, fmt.Errorf("http_port must be > 0")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("data_dir is required")
	}
	return &cfg, nil
}

// PeerIDs returns the list of peer IDs from the initial cluster, excluding
// this node's own ID.
func (c *Config) PeerIDs() []int {
	ids := make([]int, 0, len(c.InitialCluster))
	for _, p := range c.InitialCluster {
		if p.ID != c.NodeID {
			ids = append(ids, p.ID)
		}
	}
	return ids
}

// PeerMap returns the initial cluster as a map from ID to PeerConfig.
func (c *Config) PeerMap() map[int]PeerConfig {
	m := make(map[int]PeerConfig, len(c.InitialCluster))
	for _, p := range c.InitialCluster {
		m[p.ID] = p
	}
	return m
}

// HTTPAddr returns the HTTP address of this node from the initial cluster
// config, or "localhost:<port>" as a fallback.
func (c *Config) HTTPAddr() string {
	for _, p := range c.InitialCluster {
		if p.ID == c.NodeID {
			return p.HTTPAddr
		}
	}
	return fmt.Sprintf("localhost:%d", c.HTTPPort)
}

// isInitialMember returns true if this node's ID appears in the initial cluster.
func (c *Config) isInitialMember() bool {
	for _, p := range c.InitialCluster {
		if p.ID == c.NodeID {
			return true
		}
	}
	return false
}
