// Package main provides the CLI server for the Raft consensus implementation.
package main

import (
	"errors"
	"flag"
	"strings"
)

// ServerConfig holds parsed CLI configuration for the Raft server.
type ServerConfig struct {
	ID       string   // Node identifier (--id)
	Port     int      // gRPC port (--port)
	DataDir  string   // BoltDB storage path (--dir)
	Peers    []string // Bootstrap peer addresses (--peers)
	HTTPPort int      // HTTP API port (--http-port, defaults to port+1000)
	TUI      bool     // Launch TUI dashboard mode (--tui)
	Cluster  int      // Number of nodes to spawn in cluster mode (--cluster, 2-9)
}

// ParseFlags parses command-line flags into ServerConfig.
// It uses the provided flag.FlagSet to allow testing with custom arguments.
func ParseFlags(fs *flag.FlagSet, args []string) (*ServerConfig, error) {
	cfg := &ServerConfig{}

	var peersStr string

	fs.StringVar(&cfg.ID, "id", "", "Node identifier (required)")
	fs.IntVar(&cfg.Port, "port", 0, "gRPC port (required)")
	fs.StringVar(&cfg.DataDir, "dir", "", "Data directory path (required)")
	fs.StringVar(&peersStr, "peers", "", "Comma-separated list of peer addresses")
	fs.IntVar(&cfg.HTTPPort, "http-port", 0, "HTTP API port (defaults to port+1000)")
	fs.BoolVar(&cfg.TUI, "tui", false, "Launch TUI dashboard mode")
	fs.IntVar(&cfg.Cluster, "cluster", 0, "Number of nodes to spawn in cluster mode (2-9, requires --tui)")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	// Parse peers from comma-separated string
	if peersStr != "" {
		cfg.Peers = parsePeers(peersStr)
	}

	// Set default HTTP port if not specified
	if cfg.HTTPPort == 0 && cfg.Port > 0 {
		cfg.HTTPPort = cfg.Port + 1000
	}

	return cfg, nil
}

// parsePeers splits a comma-separated string into a slice of peer addresses.
// Supports both formats:
// - "localhost:5001,localhost:5002" (address only)
// - "node1=localhost:5001,node2=localhost:5002" (id=address format, extracts address)
func parsePeers(peersStr string) []string {
	if peersStr == "" {
		return nil
	}
	parts := strings.Split(peersStr, ",")
	peers := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		// Check for id=address format
		if idx := strings.Index(trimmed, "="); idx != -1 {
			// Extract address part after "="
			trimmed = strings.TrimSpace(trimmed[idx+1:])
		}
		if trimmed != "" {
			peers = append(peers, trimmed)
		}
	}
	return peers
}

// Validate checks that all required fields are present.
// Returns an error if any required field is missing.
func (c *ServerConfig) Validate() error {
	var errs []string

	// In cluster mode, --id and --peers are auto-generated
	if c.IsClusterMode() {
		// Validate cluster size (2-9)
		if c.Cluster < 2 || c.Cluster > 9 {
			errs = append(errs, "--cluster must be between 2 and 9")
		}
		// Cluster mode requires TUI
		if !c.TUI {
			errs = append(errs, "--cluster requires --tui flag")
		}
		// Port and DataDir are still required for cluster mode
		if c.Port == 0 {
			errs = append(errs, "missing required flag: --port")
		}
		if c.DataDir == "" {
			errs = append(errs, "missing required flag: --dir")
		}
	} else {
		// Single node mode validation
		if c.ID == "" {
			errs = append(errs, "missing required flag: --id")
		}
		if c.Port == 0 {
			errs = append(errs, "missing required flag: --port")
		}
		if c.DataDir == "" {
			errs = append(errs, "missing required flag: --dir")
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// IsClusterMode returns true if the server should run in cluster mode.
func (c *ServerConfig) IsClusterMode() bool {
	return c.Cluster > 0
}
