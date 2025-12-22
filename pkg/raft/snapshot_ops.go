// Package raft implements the core Raft consensus algorithm.
//
// snapshot_ops.go contains snapshot operation functionality including:
// - Handling InstallSnapshot RPCs from leaders
// - Sending InstallSnapshot RPCs to lagging followers
// - Taking local snapshots
// - Automatic snapshot triggering based on log size
// - Snapshot finalization and state machine restoration
package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/salahayoub/anchor/api"
)

// handleInstallSnapshot processes an incoming InstallSnapshot RPC and returns an InstallSnapshotResponse.
// It implements the Raft snapshot installation rules:
// - Reject if request term < currentTerm
// - Reset election timer on valid request
// - If offset == 0, start new pending snapshot
// - Write data chunk at specified offset
// - If done == true, finalize snapshot installation
func (r *Raft) handleInstallSnapshot(req *api.InstallSnapshotRequest) *api.InstallSnapshotResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &api.InstallSnapshotResponse{
		Term: r.currentTerm,
	}

	// Reject if request term < currentTerm
	if req.Term < r.currentTerm {
		return resp
	}

	// If request term > currentTerm, update term and transition to follower
	if req.Term > r.currentTerm {
		if err := r.stepDownToFollower(req.Term); err != nil {
			// On persistence failure, reject but return current term
			return resp
		}
		resp.Term = r.currentTerm
	}

	// Update leader ID
	r.leaderID = string(req.LeaderId)

	// If we're a candidate and receive InstallSnapshot with term >= currentTerm,
	// transition to follower
	if r.state == Candidate {
		r.state = Follower
	}

	// Reset election timer on valid request
	r.resetElectionTimer()

	// Check if snapshot store is configured
	if r.snapshotStore == nil {
		return resp
	}

	// If offset == 0, start new pending snapshot
	if req.Offset == 0 {
		// Clean up any existing pending snapshot
		if r.pendingSnapshot != nil && r.pendingSnapshot.file != nil {
			r.pendingSnapshot.file.Close()
			os.Remove(r.pendingSnapshot.file.Name())
		}

		// Create temp file for receiving snapshot data
		tempPath := filepath.Join(r.snapshotStore.Dir(), "install_snapshot.tmp")
		file, err := os.Create(tempPath)
		if err != nil {
			// Failed to create temp file, but don't fail the RPC
			return resp
		}

		// Initialize pending snapshot state
		r.pendingSnapshot = &pendingSnapshotState{
			meta: &SnapshotMeta{
				LastIncludedIndex: req.LastIncludedIndex,
				LastIncludedTerm:  req.LastIncludedTerm,
			},
			file:       file,
			bytesRecvd: 0,
		}
	}

	// If no pending snapshot, we can't process this chunk
	if r.pendingSnapshot == nil || r.pendingSnapshot.file == nil {
		return resp
	}

	// Verify offset matches expected position
	if int64(req.Offset) != r.pendingSnapshot.bytesRecvd {
		// Offset mismatch - abort pending snapshot and start fresh
		r.pendingSnapshot.file.Close()
		os.Remove(r.pendingSnapshot.file.Name())
		r.pendingSnapshot = nil
		return resp
	}

	// Write data chunk at specified offset
	if len(req.Data) > 0 {
		n, err := r.pendingSnapshot.file.Write(req.Data)
		if err != nil {
			// Write failed, abort pending snapshot
			r.pendingSnapshot.file.Close()
			os.Remove(r.pendingSnapshot.file.Name())
			r.pendingSnapshot = nil
			return resp
		}
		r.pendingSnapshot.bytesRecvd += int64(n)
	}

	// If done == true, finalize snapshot installation
	if req.Done {
		if err := r.finalizeSnapshotInstall(); err != nil {
			// Finalization failed, but we've already received all data
			// Clean up and return
			if r.pendingSnapshot != nil && r.pendingSnapshot.file != nil {
				r.pendingSnapshot.file.Close()
				os.Remove(r.pendingSnapshot.file.Name())
			}
			r.pendingSnapshot = nil
			return resp
		}
	}

	return resp
}

// finalizeSnapshotInstall completes the snapshot installation process.
// This is an atomic operation: we move the temp file to the final location,
// restore the state machine, and discard the log. If any step fails after
// the state machine is restored, we may have inconsistent state - but this
// is acceptable because the snapshot data is authoritative.
func (r *Raft) finalizeSnapshotInstall() error {
	if r.pendingSnapshot == nil || r.pendingSnapshot.file == nil {
		return errors.New("no pending snapshot to finalize")
	}

	// Sync and close the temp file
	if err := r.pendingSnapshot.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync snapshot file: %w", err)
	}

	tempPath := r.pendingSnapshot.file.Name()
	if err := r.pendingSnapshot.file.Close(); err != nil {
		return fmt.Errorf("failed to close snapshot file: %w", err)
	}

	// Move temp file to snapshot location via SnapshotStore
	// atomically move the temporary file to the snapshot location
	snapshotPath := filepath.Join(r.snapshotStore.Dir(), snapshotFileName)
	if err := os.Rename(tempPath, snapshotPath); err != nil {
		return fmt.Errorf("failed to move snapshot file: %w", err)
	}

	// Write metadata to meta.json
	meta := r.pendingSnapshot.meta
	meta.Size = r.pendingSnapshot.bytesRecvd

	// Compute checksum of the snapshot file
	checksum, err := computeFileChecksum(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to compute checksum: %w", err)
	}
	meta.Checksum = checksum

	// Write metadata file
	metaPath := filepath.Join(r.snapshotStore.Dir(), metaFileName)
	metaData, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	if err := os.WriteFile(metaPath, metaData, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Restore state machine from snapshot data
	// restore the state machine using StateMachine.Restore()
	snapshotFile, err := os.Open(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to open snapshot for restore: %w", err)
	}
	defer snapshotFile.Close()

	if err := r.stateMachine.Restore(snapshotFile); err != nil {
		return fmt.Errorf("failed to restore state machine: %w", err)
	}

	// Discard entire log (DeleteRange from first to last)
	// discard the entire log
	firstIndex, err := r.logStore.FirstIndex()
	if err == nil && firstIndex > 0 {
		lastIndex, err := r.logStore.LastIndex()
		if err == nil && lastIndex >= firstIndex {
			if err := r.logStore.DeleteRange(firstIndex, lastIndex); err != nil {
				// Log deletion failed, but snapshot is installed - continue anyway
				_ = err
			}
		}
	}

	// Update compacted index if the log store supports it
	if compactable, ok := r.logStore.(CompactableLogStore); ok {
		_ = compactable.SetCompactedIndex(meta.LastIncludedIndex)
	}

	// Set lastApplied and commitIndex to lastIncludedIndex
	r.lastApplied = meta.LastIncludedIndex
	r.commitIndex = meta.LastIncludedIndex

	// Update snapshot state
	r.lastSnapshotIndex = meta.LastIncludedIndex
	r.lastSnapshotTerm = meta.LastIncludedTerm

	// Update cluster configuration from snapshot
	if meta.Configuration != nil {
		r.clusterConfig = meta.Configuration

		// Update the peer list based on the new configuration
		newPeers := make([]string, 0, len(meta.Configuration.Members))
		for _, member := range meta.Configuration.Members {
			if member.ID != r.config.ID {
				addr := member.Address
				if addr == "" {
					addr = member.ID
				}
				newPeers = append(newPeers, addr)
			}
		}
		r.config.Peers = newPeers
	}

	// Clear pending snapshot state
	r.pendingSnapshot = nil

	return nil
}

// sendInstallSnapshot sends a snapshot to a lagging follower via InstallSnapshot RPC.
// Used when a follower is so far behind that the required log entries have been
// compacted away. Sends the snapshot in chunks to handle large state machines
// without requiring the entire snapshot to fit in memory.
func (r *Raft) sendInstallSnapshot(peer string) {
	r.mu.RLock()
	// Only leaders should send InstallSnapshot
	if r.state != Leader {
		r.mu.RUnlock()
		return
	}

	// Check if snapshot store is configured
	if r.snapshotStore == nil {
		r.mu.RUnlock()
		return
	}

	currentTerm := r.currentTerm
	leaderID := r.config.ID
	chunkSize := r.config.SnapshotChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultSnapshotChunkSize
	}
	r.mu.RUnlock()

	// Open the snapshot for reading
	meta, reader, err := r.snapshotStore.Open()
	if err != nil {
		// No snapshot available or error reading
		return
	}
	defer reader.Close()

	// Send snapshot in chunks
	// send in multiple chunks with incrementing offsets
	var offset uint64 = 0
	buf := make([]byte, chunkSize)
	lastChunkSent := false

	for !lastChunkSent {
		// Read a chunk of data
		n, readErr := reader.Read(buf)

		// Determine if this is the final chunk
		// set done=true on final chunk
		// We're done if we got EOF (even with data) or if we read less than buffer size
		done := readErr == io.EOF || (readErr == nil && n < chunkSize)

		// If we read nothing and got EOF, we need to send a final empty chunk
		// only if we haven't sent anything yet
		if n == 0 && readErr == io.EOF {
			if offset == 0 {
				// Empty snapshot - send one empty chunk with done=true
				done = true
			} else {
				// We've already sent all data, no need for another chunk
				break
			}
		}

		// Build InstallSnapshot request
		// include all required fields
		req := &api.InstallSnapshotRequest{
			Term:              currentTerm,
			LeaderId:          []byte(leaderID),
			LastIncludedIndex: meta.LastIncludedIndex,
			LastIncludedTerm:  meta.LastIncludedTerm,
			Offset:            offset,
			Data:              buf[:n],
			Done:              done,
		}

		// Send the chunk and wait for response
		// wait for response before sending next chunk
		resp, err := r.transport.SendInstallSnapshot(peer, req)
		if err != nil {
			// Transport failure - abort sending
			return
		}

		// Handle response
		r.handleInstallSnapshotResponse(peer, meta, resp, done)

		// Check if we should continue (still leader, term hasn't changed)
		r.mu.RLock()
		stillLeader := r.state == Leader && r.currentTerm == currentTerm
		r.mu.RUnlock()

		if !stillLeader {
			return
		}

		// Check response term
		if resp.Term > currentTerm {
			// Higher term discovered, step down
			r.mu.Lock()
			if err := r.stepDownToFollower(resp.Term); err != nil {
				log.Printf("sendInstallSnapshot: failed to step down to follower: %v", err)
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
			return
		}

		// If this was the final chunk, we're done
		if done {
			lastChunkSent = true
			break
		}

		// Update offset for next chunk
		offset += uint64(n)

		// If we hit an error other than EOF, abort
		if readErr != nil && readErr != io.EOF {
			return
		}
	}
}

// handleInstallSnapshotResponse processes the response from an InstallSnapshot RPC.
// On success with done=true, it updates nextIndex to lastIncludedIndex + 1.
// On failure (higher term), it steps down to follower.
func (r *Raft) handleInstallSnapshotResponse(peer string, meta *SnapshotMeta, resp *api.InstallSnapshotResponse, done bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If we're no longer the leader, ignore the response
	if r.state != Leader {
		return
	}

	// If response term is higher, step down to follower
	if resp.Term > r.currentTerm {
		if err := r.stepDownToFollower(resp.Term); err != nil {
			log.Printf("handleInstallSnapshotResponse: failed to step down to follower: %v", err)
			return
		}
		return
	}

	// On success with done=true, update nextIndex to lastIncludedIndex + 1
	// update nextIndex after successful snapshot installation
	if done {
		r.nextIndex[peer] = meta.LastIncludedIndex + 1
		r.matchIndex[peer] = meta.LastIncludedIndex
	}
}

// Snapshot takes a snapshot of the current state machine state and persists it.
// This can be called on any node (leader or follower) to take a local snapshot.
// The snapshot includes the state machine data, lastApplied index and term,
// and the current cluster configuration.
func (r *Raft) Snapshot() (*SnapshotMeta, error) {
	r.mu.Lock()

	// Check if snapshot store is configured
	if r.snapshotStore == nil {
		r.mu.Unlock()
		return nil, ErrNoSnapshotStore
	}

	// Check if running
	if !r.running {
		r.mu.Unlock()
		return nil, ErrStopped
	}

	// Capture current state for snapshot
	// snapshot can be taken on any node
	lastApplied := r.lastApplied

	// Get the term at lastApplied
	// If lastApplied is 0, term is 0
	var termAtLastApplied uint64
	if lastApplied > 0 {
		entry, err := r.logStore.GetLog(lastApplied)
		if err != nil {
			r.mu.Unlock()
			return nil, fmt.Errorf("failed to get log entry at lastApplied: %w", err)
		}
		termAtLastApplied = entry.Term
	}

	// Copy cluster configuration
	// include cluster configuration in snapshot
	var config *ClusterConfig
	if r.clusterConfig != nil {
		config = &ClusterConfig{
			Members: make([]ClusterMember, len(r.clusterConfig.Members)),
		}
		copy(config.Members, r.clusterConfig.Members)
	}

	r.mu.Unlock()

	// Capture state machine state
	// capture state machine state using StateMachine.Snapshot()
	stateReader, err := r.stateMachine.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to capture state machine snapshot: %w", err)
	}
	defer stateReader.Close()

	// Read all state data
	stateData, err := io.ReadAll(stateReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read state machine snapshot: %w", err)
	}

	// Create snapshot metadata
	// persist snapshot data along with metadata
	meta := &SnapshotMeta{
		LastIncludedIndex: lastApplied,
		LastIncludedTerm:  termAtLastApplied,
		Configuration:     config,
	}

	// Create snapshot in store
	sink, err := r.snapshotStore.Create(meta)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Write state data to snapshot
	if _, err := sink.Write(stateData); err != nil {
		sink.Cancel()
		return nil, fmt.Errorf("failed to write snapshot data: %w", err)
	}

	// Close to finalize the snapshot
	if err := sink.Close(); err != nil {
		return nil, fmt.Errorf("failed to finalize snapshot: %w", err)
	}

	// Update snapshot state
	// return snapshot metadata to caller
	r.mu.Lock()
	r.lastSnapshotIndex = lastApplied
	r.lastSnapshotTerm = termAtLastApplied
	r.mu.Unlock()

	// Get the final metadata (with size and checksum populated)
	finalMeta, err := r.snapshotStore.GetMeta()
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot metadata: %w", err)
	}

	return finalMeta, nil
}

// maybeSnapshot checks if the log size exceeds the threshold and triggers
// an automatic snapshot in a background goroutine.
// Snapshots are taken asynchronously to avoid blocking the main Raft loop,
// which must continue processing RPCs and heartbeats.
func (r *Raft) maybeSnapshot() {
	// Skip if automatic snapshots are disabled (threshold is 0)
	if r.config.SnapshotThreshold == 0 {
		return
	}

	// Skip if no snapshot store is configured
	if r.snapshotStore == nil {
		return
	}

	// Skip if a snapshot is already in progress
	if r.snapshotting {
		return
	}

	// Check if log size exceeds threshold
	// Log size = lastApplied - lastSnapshotIndex
	logSize := r.lastApplied - r.lastSnapshotIndex
	if logSize <= r.config.SnapshotThreshold {
		return
	}

	// Mark snapshot as in progress
	r.snapshotting = true

	// Trigger snapshot in background goroutine to avoid blocking
	// continue processing client requests and RPCs without blocking
	go r.runAutoSnapshot()
}

// runAutoSnapshot runs an automatic snapshot in the background.
// It captures the state machine state and persists it to the snapshot store,
// then compacts the log.
func (r *Raft) runAutoSnapshot() {
	// Take the snapshot (this method handles its own locking)
	_, err := r.Snapshot()
	if err != nil {
		// Snapshot failed, clear the snapshotting flag
		r.mu.Lock()
		r.snapshotting = false
		r.mu.Unlock()
		return
	}

	// Compact the log after successful snapshot
	// delete all log entries up to and including lastSnapshotIndex
	if err := r.CompactLog(); err != nil {
		// Log compaction failed, but snapshot succeeded
		// Clear the snapshotting flag anyway
		r.mu.Lock()
		r.snapshotting = false
		r.mu.Unlock()
		return
	}

	// Clear the snapshotting flag
	r.mu.Lock()
	r.snapshotting = false
	r.mu.Unlock()
}
