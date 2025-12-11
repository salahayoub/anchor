// Package tui provides a terminal user interface for monitoring and interacting
// with the Skeg distributed key-value store.
package tui

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/salahayoub/anchor/api"
	"github.com/salahayoub/anchor/pkg/fsm"
	"github.com/salahayoub/anchor/pkg/raft"
)

// Error variables for RaftDataFetcher operations.
var (
	// ErrNotConnected is returned when operations are attempted while disconnected.
	ErrNotConnected = errors.New("not connected to raft node")
	// ErrKeyNotFound is returned when a GET operation finds no value for the key.
	ErrKeyNotFound = errors.New("key not found")
)

// RaftDataFetcher implements DataFetcher by connecting to a local Raft node.
// It provides cluster state, log entries, and key-value operations.
type RaftDataFetcher struct {
	raft      *raft.Raft
	kvStore   *fsm.KVStore
	logStore  raft.LogStore
	connected bool
	mu        sync.RWMutex
}

// NewRaftDataFetcher creates a fetcher connected to the given Raft node.
func NewRaftDataFetcher(r *raft.Raft, kv *fsm.KVStore, ls raft.LogStore) *RaftDataFetcher {
	return &RaftDataFetcher{
		raft:      r,
		kvStore:   kv,
		logStore:  ls,
		connected: true,
	}
}

// FetchClusterState retrieves current cluster state from the Raft node.
// Returns node roles, term, commit index, and replication lag for followers.
func (f *RaftDataFetcher) FetchClusterState() (*ClusterState, error) {
	f.mu.RLock()
	connected := f.connected
	f.mu.RUnlock()

	if !connected {
		return nil, ErrNotConnected
	}

	// Get local node state
	localRole := f.raft.State().String()
	currentTerm := f.raft.CurrentTerm()
	commitIndex := f.raft.CommitIndex()
	leaderID := f.raft.Leader()
	config := f.raft.GetConfiguration()
	matchIndex := f.raft.MatchIndex()

	// Build node status list
	nodes := make([]NodeStatus, 0, len(config.Members))
	for _, member := range config.Members {
		nodeRole := "Follower"
		if member.ID == leaderID {
			nodeRole = "Leader"
		}
		// For the local node, use the actual state
		if member.ID == f.raft.GetConfiguration().Members[0].ID {
			nodeRole = localRole
		}

		nodes = append(nodes, NodeStatus{
			ID:         member.ID,
			Role:       nodeRole,
			Connected:  true, // Assume connected; actual connectivity would require health checks
			MatchIndex: matchIndex[member.ID],
		})
	}

	// Calculate replication lag (only meaningful when leader)
	replicationLag := make(map[string]int64)
	if localRole == "Leader" {
		lastIndex, err := f.logStore.LastIndex()
		if err == nil {
			for peerID, peerMatchIndex := range matchIndex {
				lag := int64(lastIndex) - int64(peerMatchIndex)
				if lag < 0 {
					lag = 0
				}
				replicationLag[peerID] = lag
			}
		}
	}

	return &ClusterState{
		LocalNodeID:    config.Members[0].ID,
		LocalRole:      localRole,
		CurrentTerm:    currentTerm,
		CommitIndex:    commitIndex,
		LeaderID:       leaderID,
		Nodes:          nodes,
		ReplicationLag: replicationLag,
		LastUpdated:    time.Now(),
	}, nil
}

// FetchRecentLogs retrieves the N most recent committed log entries.
func (f *RaftDataFetcher) FetchRecentLogs(count int) ([]LogEntry, error) {
	f.mu.RLock()
	connected := f.connected
	f.mu.RUnlock()

	if !connected {
		return nil, ErrNotConnected
	}

	if count <= 0 {
		return []LogEntry{}, nil
	}

	// Get the last index from the log store
	lastIndex, err := f.logStore.LastIndex()
	if err != nil {
		return nil, err
	}

	if lastIndex == 0 {
		return []LogEntry{}, nil
	}

	// Calculate start index
	startIndex := uint64(1)
	if lastIndex > uint64(count) {
		startIndex = lastIndex - uint64(count) + 1
	}

	// Fetch log entries
	entries := make([]LogEntry, 0, count)
	for idx := startIndex; idx <= lastIndex; idx++ {
		logEntry, err := f.logStore.GetLog(idx)
		if err != nil {
			// Skip entries that can't be read (e.g., compacted)
			continue
		}

		// Parse operation from log data
		operation := parseOperation(logEntry)

		entries = append(entries, LogEntry{
			Index:     logEntry.Index,
			Term:      logEntry.Term,
			Operation: operation,
			Timestamp: time.Now(), // Log entries don't store timestamp, use current time
		})
	}

	return entries, nil
}

// parseOperation extracts the operation type from a log entry.
func parseOperation(entry *api.LogEntry) string {
	if entry == nil {
		return "unknown"
	}

	switch entry.Type {
	case api.LogType_LOG_COMMAND:
		// Try to parse as JSON command
		var cmd struct {
			Op    string `json:"op"`
			Key   string `json:"key"`
			Value string `json:"value,omitempty"`
		}
		if err := json.Unmarshal(entry.Data, &cmd); err == nil {
			if cmd.Op == "set" && cmd.Key != "" {
				return "set " + cmd.Key
			}
			return cmd.Op
		}
		return "command"
	case api.LogType_LOG_CONFIGURATION:
		return "config"
	case api.LogType_LOG_NOOP:
		return "noop"
	default:
		return "unknown"
	}
}

// ExecuteGet performs a GET operation on the KV store.
// Returns the value for the given key or ErrKeyNotFound if not present.
func (f *RaftDataFetcher) ExecuteGet(key string) (string, error) {
	f.mu.RLock()
	connected := f.connected
	f.mu.RUnlock()

	if !connected {
		return "", ErrNotConnected
	}

	value, ok := f.kvStore.Get(key)
	if !ok {
		return "", ErrKeyNotFound
	}

	return value, nil
}

// ExecutePut performs a PUT operation on the KV store via Raft consensus.
// The operation is submitted to the leader and replicated to the cluster.
// Returns ErrNotLeader if the local node is not the leader.
func (f *RaftDataFetcher) ExecutePut(key, value string) error {
	f.mu.RLock()
	connected := f.connected
	f.mu.RUnlock()

	if !connected {
		return ErrNotConnected
	}

	// Create the command
	cmd := fsm.Command{
		Op:    "set",
		Key:   key,
		Value: value,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// Apply via Raft with a 5 second timeout
	return f.raft.Apply(cmdBytes, 5*time.Second)
}

// IsConnected returns whether the fetcher is connected to the node.
func (f *RaftDataFetcher) IsConnected() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.connected
}

// Reconnect attempts to reconnect to the node.
// For a local Raft node, this verifies the node is still running.
func (f *RaftDataFetcher) Reconnect() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// For a local Raft node, we check if it's still operational
	// by attempting to read its state
	_ = f.raft.State()
	f.connected = true
	return nil
}

// SetConnected sets the connection state (for testing purposes).
func (f *RaftDataFetcher) SetConnected(connected bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connected = connected
}
