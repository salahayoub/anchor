// Package tui provides a terminal user interface for monitoring and interacting
// with the Skeg distributed key-value store.
package tui

import (
	"time"
)

// ClusterState represents the current state of the Raft cluster.
type ClusterState struct {
	LocalNodeID    string
	LocalRole      string // "Leader", "Follower", "Candidate"
	CurrentTerm    uint64
	CommitIndex    uint64
	LeaderID       string
	Nodes          []NodeStatus
	ReplicationLag map[string]int64 // follower ID -> entries behind
	LastUpdated    time.Time
}

// NodeStatus represents the status of a single node.
type NodeStatus struct {
	ID         string
	Role       string
	Connected  bool
	MatchIndex uint64
}

// LogEntry represents a single log entry for display.
type LogEntry struct {
	Index     uint64
	Term      uint64
	Operation string
	Timestamp time.Time
}

// DataFetcher defines the interface for retrieving cluster data.
// This interface enables testability by allowing mock implementations.
type DataFetcher interface {
	// FetchClusterState retrieves current cluster state.
	FetchClusterState() (*ClusterState, error)

	// FetchRecentLogs retrieves the N most recent committed log entries.
	FetchRecentLogs(count int) ([]LogEntry, error)

	// ExecuteGet performs a GET operation on the KV store.
	ExecuteGet(key string) (string, error)

	// ExecutePut performs a PUT operation on the KV store.
	ExecutePut(key, value string) error

	// IsConnected returns whether the fetcher is connected to the node.
	IsConnected() bool

	// Reconnect attempts to reconnect to the node.
	Reconnect() error
}
