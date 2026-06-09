package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	NodeID         int          `json:"node_id"`
	HTTPPort       int          `json:"http_port"`
	DataDir        string       `json:"data_dir"`
	InitialCluster []PeerConfig `json:"initial_cluster"`
}

type PeerConfig struct {
	ID       int    `json:"id"`
	HTTPAddr string `json:"http_addr"`
	RaftAddr string `json:"raft_addr"`
}

func LoadConfig(path string) (*Config, error) {
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

func (c *Config) PeerIDs() []int {
	ids := make([]int, 0, len(c.InitialCluster))
	for _, p := range c.InitialCluster {
		if p.ID != c.NodeID {
			ids = append(ids, p.ID)
		}
	}
	return ids
}

func (c *Config) PeerMap() map[int]PeerConfig {
	m := make(map[int]PeerConfig, len(c.InitialCluster))
	for _, p := range c.InitialCluster {
		m[p.ID] = p
	}
	return m
}

func (c *Config) HTTPAddr() string {
	for _, p := range c.InitialCluster {
		if p.ID == c.NodeID {
			return p.HTTPAddr
		}
	}
	return fmt.Sprintf("localhost:%d", c.HTTPPort)
}

func (c *Config) IsInitialMember() bool {
	for _, p := range c.InitialCluster {
		if p.ID == c.NodeID {
			return true
		}
	}
	return false
}
