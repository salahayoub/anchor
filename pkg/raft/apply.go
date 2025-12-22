// Package raft implements the core Raft consensus algorithm.
//
// apply.go contains log entry application functionality including:
// - Applying committed entries to the state machine
// - Applying configuration entries to update cluster membership
// - Reconstructing cluster configuration from log on restart
// - Replaying log entries after restart
// - Client command submission (Apply)
package raft

import (
	"fmt"
	"time"

	"github.com/salahayoub/anchor/api"
)

// applyEntries applies all committed but not yet applied entries to the state machine.
// Entries are applied in strictly increasing index order to maintain determinism.
// This separation of commit and apply allows the state machine to be updated
// asynchronously from the consensus protocol.
func (r *Raft) applyEntries() {
	// Apply all entries from lastApplied + 1 to commitIndex in order
	for r.lastApplied < r.commitIndex {
		nextIndex := r.lastApplied + 1

		// Get the log entry to apply
		entry, err := r.logStore.GetLog(nextIndex)
		if err != nil {
			// Log error but continue - entry might not exist
			return
		}

		// Apply the entry based on its type
		switch entry.Type {
		case api.LogType_LOG_COMMAND:
			// Apply command entries to the state machine
			r.stateMachine.Apply(entry.Data)
		case api.LogType_LOG_CONFIGURATION:
			// Apply configuration entries to update cluster membership
			r.applyConfigEntry(entry)
		}
		// LOG_NOOP entries are applied but have no effect

		// Increment lastApplied after successful application
		r.lastApplied = nextIndex
	}

	// Check if automatic snapshot should be triggered
	r.maybeSnapshot()
}

// applyConfigEntry applies a LOG_CONFIGURATION entry to update cluster membership.
// It deserializes the ClusterConfiguration from the entry data and updates
// the in-memory clusterConfig and peer list.
// This method assumes the caller holds the mutex.
func (r *Raft) applyConfigEntry(entry *api.LogEntry) {
	if entry == nil || entry.Type != api.LogType_LOG_CONFIGURATION {
		return
	}

	// Deserialize the cluster configuration from entry data
	config, err := DeserializeConfig(entry.Data)
	if err != nil {
		// Log error but continue - malformed config entry
		return
	}

	// Update the in-memory cluster configuration
	r.clusterConfig = config

	// Update the peer list based on the new configuration
	// Peers are all members except self
	newPeers := make([]string, 0, len(config.Members))
	for _, member := range config.Members {
		if member.ID != r.config.ID {
			// Use Address if available, otherwise use ID
			addr := member.Address
			if addr == "" {
				addr = member.ID
			}
			newPeers = append(newPeers, addr)
		}
	}
	r.config.Peers = newPeers

	// If we're the leader, ensure nextIndex/matchIndex are initialized for new peers
	if r.state == Leader {
		lastIndex, err := r.logStore.LastIndex()
		if err != nil {
			lastIndex = 0
		}
		for _, peer := range newPeers {
			if _, exists := r.nextIndex[peer]; !exists {
				r.nextIndex[peer] = lastIndex + 1
				r.matchIndex[peer] = 0
			}
		}
	}
}

// reconstructConfigFromLog scans committed log entries for LOG_CONFIGURATION type
// and applies them in order to rebuild cluster membership on restart.
// This is necessary because cluster membership is stored in the log itself,
// not in a separate configuration file, enabling atomic membership changes.
func (r *Raft) reconstructConfigFromLog() error {
	// Get the range of log entries to scan
	firstIndex, err := r.logStore.FirstIndex()
	if err != nil {
		return err
	}

	lastIndex, err := r.logStore.LastIndex()
	if err != nil {
		return err
	}

	// If log is empty, keep the bootstrap configuration
	if firstIndex == 0 || lastIndex == 0 {
		return nil
	}

	// Track if we found any config entries
	foundConfig := false

	// Scan all log entries in order and apply config entries
	// apply in log order to maintain consistency
	for idx := firstIndex; idx <= lastIndex; idx++ {
		entry, err := r.logStore.GetLog(idx)
		if err != nil {
			// Skip entries that can't be read
			continue
		}

		if entry.Type == api.LogType_LOG_CONFIGURATION {
			// Deserialize and apply the config entry
			config, err := DeserializeConfig(entry.Data)
			if err != nil {
				// Skip malformed config entries
				continue
			}

			// Update the in-memory cluster configuration
			r.clusterConfig = config

			// Update the peer list based on the new configuration
			newPeers := make([]string, 0, len(config.Members))
			for _, member := range config.Members {
				if member.ID != r.config.ID {
					addr := member.Address
					if addr == "" {
						addr = member.ID
					}
					newPeers = append(newPeers, addr)
				}
			}
			r.config.Peers = newPeers

			foundConfig = true
		}
	}

	// If we found config entries, the membership has been reconstructed
	// If not, we keep the bootstrap configuration from NewRaft
	_ = foundConfig

	return nil
}

// replayLogEntries replays all committed log entries from lastApplied+1 to the last log index.
// Called during startup to rebuild state machine state from the log. This handles the case
// where entries were committed but the node crashed before taking a snapshot.
func (r *Raft) replayLogEntries() error {
	lastIndex, err := r.logStore.LastIndex()
	if err != nil {
		return err
	}

	// If no log entries, nothing to replay
	if lastIndex == 0 {
		return nil
	}

	// Set commitIndex to lastIndex since all persisted entries were committed
	// (entries are only persisted after being committed in a previous run)
	if lastIndex > r.commitIndex {
		r.commitIndex = lastIndex
	}

	// Apply all entries from lastApplied+1 to commitIndex
	for r.lastApplied < r.commitIndex {
		nextIndex := r.lastApplied + 1
		entry, err := r.logStore.GetLog(nextIndex)
		if err != nil {
			return fmt.Errorf("failed to get log entry %d: %w", nextIndex, err)
		}

		// Apply command entries to state machine
		if entry.Type == api.LogType_LOG_COMMAND {
			r.stateMachine.Apply(entry.Data)
		}

		r.lastApplied = nextIndex
	}

	return nil
}

// Apply submits a command to be replicated (leader only).
// It appends the command to the log and waits for commit or timeout.
func (r *Raft) Apply(cmd []byte, timeout time.Duration) error {
	r.mu.Lock()

	// Reject if not leader
	if r.state != Leader {
		leaderID := r.leaderID
		r.mu.Unlock()
		return &NotLeaderError{LeaderHint: leaderID}
	}

	// Check if running
	if !r.running {
		r.mu.Unlock()
		return ErrStopped
	}

	// Get next log index
	lastIndex, err := r.logStore.LastIndex()
	if err != nil {
		r.mu.Unlock()
		return err
	}
	newIndex := lastIndex + 1

	// Create log entry
	entry := &api.LogEntry{
		Index: newIndex,
		Term:  r.currentTerm,
		Type:  api.LogType_LOG_COMMAND,
		Data:  cmd,
	}

	// Append to log
	if err := r.logStore.StoreLogs([]*api.LogEntry{entry}); err != nil {
		r.mu.Unlock()
		return err
	}

	// Try to advance commit index immediately (handles single-node cluster case)
	r.advanceCommitIndex()

	r.mu.Unlock()

	// Wait for commit or timeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			return ErrTimeout
		case <-ticker.C:
			r.mu.RLock()
			committed := r.commitIndex >= newIndex
			isLeader := r.state == Leader
			r.mu.RUnlock()

			if committed {
				return nil
			}
			if !isLeader {
				return ErrNotLeader
			}
		}
	}
}
