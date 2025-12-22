// Package raft implements the core Raft consensus algorithm.
//
// replication.go contains log replication related functionality including:
// - Sending AppendEntries RPCs to followers
// - Handling AppendEntries requests and responses
// - Advancing commit index based on replication progress
// - Non-voter promotion logic
// - Election timer management
package raft

import (
	"log"

	"github.com/salahayoub/anchor/api"
)

// sendAppendEntries sends an AppendEntries RPC to a specific peer.
// This method builds the request with prevLogIndex, prevLogTerm, entries from nextIndex,
// and leaderCommit, then sends it via Transport.
// If the peer's nextIndex is less than the first log index (after compaction),
// it sends InstallSnapshot instead.
func (r *Raft) sendAppendEntries(peer string) {
	r.mu.RLock()
	// Only leaders should send AppendEntries
	if r.state != Leader {
		r.mu.RUnlock()
		return
	}

	// Get nextIndex for this peer
	nextIdx, ok := r.nextIndex[peer]
	if !ok {
		r.mu.RUnlock()
		return
	}

	// Check if peer needs a snapshot instead of AppendEntries
	// if nextIndex < firstLogIndex, send InstallSnapshot
	firstLogIndex, err := r.logStore.FirstIndex()
	if err == nil && firstLogIndex > 0 && nextIdx < firstLogIndex {
		// Peer is too far behind, need to send snapshot
		r.mu.RUnlock()
		go r.sendInstallSnapshot(peer)
		return
	}

	// Build the request
	var prevLogIndex uint64
	var prevLogTerm uint64

	// Get prevLogIndex and prevLogTerm
	if nextIdx > 1 {
		prevLogIndex = nextIdx - 1
		entry, err := r.logStore.GetLog(prevLogIndex)
		if err == nil {
			prevLogTerm = entry.Term
		} else {
			// If we can't get the prevLog entry (possibly compacted),
			// we need to send a snapshot instead
			r.mu.RUnlock()
			go r.sendInstallSnapshot(peer)
			return
		}
	}

	// Get entries from nextIndex onward
	var entries []*api.LogEntry
	lastIndex, err := r.logStore.LastIndex()
	if err == nil && lastIndex >= nextIdx {
		for i := nextIdx; i <= lastIndex; i++ {
			entry, err := r.logStore.GetLog(i)
			if err != nil {
				break
			}
			entries = append(entries, entry)
		}
	}

	// Build AppendEntriesRequest
	req := &api.AppendEntriesRequest{
		Term:         r.currentTerm,
		LeaderId:     []byte(r.config.ID),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}

	r.mu.RUnlock()

	// Send the request asynchronously
	go func() {
		resp, err := r.transport.SendAppendEntries(peer, req)
		if err != nil {
			// Transport failure - will retry on next heartbeat
			return
		}
		r.handleAppendEntriesResponse(peer, req, resp)
	}()
}

// handleAppendEntries processes an incoming AppendEntries RPC and returns an AppendEntriesResponse.
// It implements the Raft log replication rules:
// - Reject if request term < currentTerm
// - Reject if log doesn't contain entry at prevLogIndex with prevLogTerm
// - Delete conflicting entries and append new entries
// - Update commitIndex based on leaderCommit
// - Reset election timeout on valid request
// - Persist log entries before responding
//
// PERSISTENCE ORDERING:
// This method ensures all persistence happens BEFORE the response is returned:
// - If term changes, it is persisted via stepDownToFollower() before any other processing
// - Log entries are persisted to LogStore via StoreLogs() before returning success
// - On persistence failure, the response indicates failure and state is not modified
func (r *Raft) handleAppendEntries(req *api.AppendEntriesRequest) *api.AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &api.AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: false,
	}

	// If request term > currentTerm, update term and transition to follower
	if req.Term > r.currentTerm {
		if err := r.stepDownToFollower(req.Term); err != nil {
			// On persistence failure, reject but return current term
			return resp
		}
		resp.Term = r.currentTerm
	}

	// Reject if request term < currentTerm
	if req.Term < r.currentTerm {
		return resp
	}

	// Update leader ID
	r.leaderID = string(req.LeaderId)

	// If we're a candidate and receive AppendEntries with term >= currentTerm,
	// transition to follower
	if r.state == Candidate {
		r.state = Follower
	}

	// Reset election timeout on valid request
	// This is signaled by setting a flag that the main loop will handle
	r.resetElectionTimer()

	// Perform log consistency check at prevLogIndex/prevLogTerm
	// Handle case where prevLogIndex is covered by snapshot
	if req.PrevLogIndex > 0 {
		// If prevLogIndex is at or before the snapshot, we can trust the snapshot
		// The snapshot contains all entries up to lastSnapshotIndex, so if
		// prevLogIndex <= lastSnapshotIndex, the consistency check passes if
		// the snapshot's term matches (or we trust the leader's view)
		if req.PrevLogIndex <= r.lastSnapshotIndex {
			// prevLogIndex is covered by snapshot
			// If prevLogIndex == lastSnapshotIndex, verify term matches snapshot term
			if req.PrevLogIndex == r.lastSnapshotIndex && req.PrevLogTerm != r.lastSnapshotTerm {
				// Term mismatch at snapshot boundary
				lastIndex, _ := r.logStore.LastIndex()
				resp.LastLogIndex = lastIndex
				return resp
			}
			// Otherwise, trust the snapshot covers this index correctly
		} else {
			// prevLogIndex is after snapshot, check the log
			entry, err := r.logStore.GetLog(req.PrevLogIndex)
			if err != nil {
				// Log doesn't contain entry at prevLogIndex
				lastIndex, _ := r.logStore.LastIndex()
				resp.LastLogIndex = lastIndex
				return resp
			}
			if entry.Term != req.PrevLogTerm {
				// Entry at prevLogIndex has different term
				lastIndex, _ := r.logStore.LastIndex()
				resp.LastLogIndex = lastIndex
				return resp
			}
		}
	}

	// Delete conflicting entries and append new entries
	if len(req.Entries) > 0 {
		// Filter out entries that conflict with snapshot
		// Skip entries with index <= lastSnapshotIndex as they are already
		// incorporated into the snapshot and cannot be modified
		filteredEntries := make([]*api.LogEntry, 0, len(req.Entries))
		for _, entry := range req.Entries {
			if entry.Index > r.lastSnapshotIndex {
				filteredEntries = append(filteredEntries, entry)
			}
			// Entries with index <= lastSnapshotIndex are silently skipped
		}

		if len(filteredEntries) > 0 {
			// Find the first conflicting entry among filtered entries
			newEntryIndex := 0
			for i, entry := range filteredEntries {
				existingEntry, err := r.logStore.GetLog(entry.Index)
				if err != nil {
					// Entry doesn't exist, start appending from here
					newEntryIndex = i
					break
				}
				if existingEntry.Term != entry.Term {
					// Conflict found - delete this and all following entries
					lastIndex, _ := r.logStore.LastIndex()
					if err := r.logStore.DeleteRange(entry.Index, lastIndex); err != nil {
						return resp
					}
					newEntryIndex = i
					break
				}
				// Entry matches, continue to next
				newEntryIndex = i + 1
			}

			// Append new entries (persist before responding)
			if newEntryIndex < len(filteredEntries) {
				entriesToStore := filteredEntries[newEntryIndex:]
				if err := r.logStore.StoreLogs(entriesToStore); err != nil {
					return resp
				}
			}
		}
	}

	// Update commitIndex based on leaderCommit
	if req.LeaderCommit > r.commitIndex {
		// Set commitIndex to min(leaderCommit, index of last new entry)
		lastNewIndex := req.PrevLogIndex
		if len(req.Entries) > 0 {
			lastNewIndex = req.Entries[len(req.Entries)-1].Index
		}
		if req.LeaderCommit < lastNewIndex {
			r.commitIndex = req.LeaderCommit
		} else {
			r.commitIndex = lastNewIndex
		}

		// Apply newly committed entries to the state machine
		r.applyEntries()
	}

	// Get last log index for response
	lastIndex, _ := r.logStore.LastIndex()
	resp.LastLogIndex = lastIndex
	resp.Success = true
	return resp
}

// handleAppendEntriesResponse processes the response from an AppendEntries RPC.
// On success, it updates matchIndex and nextIndex for the peer.
// On failure due to log inconsistency, it decrements nextIndex and schedules retry.
// After updating matchIndex, checks if peer is NonVoter and eligible for promotion.
func (r *Raft) handleAppendEntriesResponse(peer string, req *api.AppendEntriesRequest, resp *api.AppendEntriesResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If we're no longer the leader, ignore the response
	if r.state != Leader {
		return
	}

	// If response term is higher, step down to follower
	if resp.Term > r.currentTerm {
		if err := r.stepDownToFollower(resp.Term); err != nil {
			log.Printf("handleAppendEntriesResponse: failed to step down to follower: %v", err)
			return
		}
		return
	}

	// Ignore responses from old terms
	if resp.Term != r.currentTerm {
		return
	}

	if resp.Success {
		// Update matchIndex and nextIndex on success
		// matchIndex = prevLogIndex + len(entries)
		newMatchIndex := req.PrevLogIndex + uint64(len(req.Entries))
		if newMatchIndex > r.matchIndex[peer] {
			r.matchIndex[peer] = newMatchIndex
		}
		// nextIndex = matchIndex + 1
		r.nextIndex[peer] = r.matchIndex[peer] + 1

		// Try to advance commit index after successful replication
		r.advanceCommitIndex()

		// Check if peer is NonVoter and eligible for promotion
		r.checkAndPromoteNonVoter(peer)
	} else {
		// Decrement nextIndex on failure and schedule retry
		if r.nextIndex[peer] > 1 {
			r.nextIndex[peer]--
		}
		// Schedule retry by sending another AppendEntries
		// Release lock before calling sendAppendEntries to avoid deadlock
		go func() {
			r.sendAppendEntries(peer)
		}()
	}
}

// advanceCommitIndex calculates the majority matchIndex among voters and advances
// commitIndex if the entry's term equals the current term.
// The current-term check is a critical safety property: committing entries from
// previous terms directly could cause committed entries to be lost if a leader
// with a conflicting log is elected. By only committing current-term entries,
// previous-term entries get committed indirectly (they precede the current-term entry).
func (r *Raft) advanceCommitIndex() {
	// Only leaders can advance commit index this way
	if r.state != Leader {
		return
	}

	// Collect all matchIndex values from voters only (including self if voter)
	// Non-voters do not participate in commit quorum calculations
	lastIndex, err := r.logStore.LastIndex()
	if err != nil {
		return
	}

	// Get list of voters
	voters := r.getVotersUnlocked()
	if len(voters) == 0 {
		return
	}

	// Build list of match indices from voters only
	matchIndices := make([]uint64, 0, len(voters))
	for _, voterID := range voters {
		if voterID == r.config.ID {
			// Leader's own matchIndex is effectively lastIndex
			matchIndices = append(matchIndices, lastIndex)
		} else {
			// Get matchIndex for voter peer
			matchIndices = append(matchIndices, r.matchIndex[voterID])
		}
	}

	// Calculate majority based on voter count only
	majority := calculateQuorum(voters)

	// Find the highest index that is replicated on a majority of voters
	// Sort in descending order and take the (majority-1)th element
	sortedIndices := make([]uint64, len(matchIndices))
	copy(sortedIndices, matchIndices)

	// Simple selection: find the majority-th largest element
	// We need at least 'majority' voters to have matchIndex >= N
	// So we find the (majority)th largest matchIndex
	for i := 0; i < majority; i++ {
		maxIdx := i
		for j := i + 1; j < len(sortedIndices); j++ {
			if sortedIndices[j] > sortedIndices[maxIdx] {
				maxIdx = j
			}
		}
		sortedIndices[i], sortedIndices[maxIdx] = sortedIndices[maxIdx], sortedIndices[i]
	}

	// The majority-th largest value (0-indexed: majority-1)
	majorityIndex := sortedIndices[majority-1]

	// Only advance if majorityIndex > commitIndex
	if majorityIndex <= r.commitIndex {
		return
	}

	// Only commit entries from the current term (Raft Figure 8 safety property).
	// Entries from previous terms are committed indirectly when a current-term
	// entry that follows them is committed.
	entry, err := r.logStore.GetLog(majorityIndex)
	if err != nil {
		return
	}

	if entry.Term != r.currentTerm {
		return
	}

	// Advance commitIndex
	r.commitIndex = majorityIndex

	// Apply newly committed entries
	r.applyEntries()
}

// checkAndPromoteNonVoter checks if a peer is a NonVoter that has caught up
// and should be promoted to Voter status.
// NonVoters are promoted when matchIndex >= commitIndex, meaning they have
// all committed entries. This ensures new voters won't slow down commits.
func (r *Raft) checkAndPromoteNonVoter(peer string) {
	// Only leaders can promote non-voters
	if r.state != Leader {
		return
	}

	// Find the peer in the cluster config
	if r.clusterConfig == nil {
		return
	}

	var peerMember *ClusterMember
	peerIndex := -1
	for i := range r.clusterConfig.Members {
		if r.clusterConfig.Members[i].ID == peer || r.clusterConfig.Members[i].Address == peer {
			peerMember = &r.clusterConfig.Members[i]
			peerIndex = i
			break
		}
	}

	// If peer not found or already a Voter, nothing to do
	if peerMember == nil || peerMember.State == Voter {
		return
	}

	// Check if NonVoter's matchIndex >= commitIndex
	// promote when matchIndex reaches commitIndex
	matchIdx := r.matchIndex[peer]
	if matchIdx < r.commitIndex {
		return
	}

	// Create new cluster config with the peer promoted to Voter
	newMembers := make([]ClusterMember, len(r.clusterConfig.Members))
	copy(newMembers, r.clusterConfig.Members)
	newMembers[peerIndex].State = Voter
	newConfig := &ClusterConfig{Members: newMembers}

	// Serialize the new config
	configData, err := SerializeConfig(newConfig)
	if err != nil {
		// Log error but continue - promotion will be retried
		return
	}

	// Get next log index
	lastIndex, err := r.logStore.LastIndex()
	if err != nil {
		return
	}
	newIndex := lastIndex + 1

	// Create LOG_CONFIG entry for promotion
	entry := &api.LogEntry{
		Index: newIndex,
		Term:  r.currentTerm,
		Type:  api.LogType_LOG_CONFIGURATION,
		Data:  configData,
	}

	// Append entry to log
	if err := r.logStore.StoreLogs([]*api.LogEntry{entry}); err != nil {
		return
	}

	// Note: The config will be applied when the entry is committed and applied
	// through the normal applyEntries flow
}

// getLastLogInfo returns the index and term of the last log entry.
// Returns (0, 0) if the log is empty.
func (r *Raft) getLastLogInfo() (uint64, uint64, error) {
	lastIndex, err := r.logStore.LastIndex()
	if err != nil {
		return 0, 0, err
	}

	if lastIndex == 0 {
		return 0, 0, nil
	}

	entry, err := r.logStore.GetLog(lastIndex)
	if err != nil {
		return 0, 0, err
	}

	return entry.Index, entry.Term, nil
}

// resetElectionTimer resets the election timer to a new random timeout.
// This method assumes the caller holds the mutex.
func (r *Raft) resetElectionTimer() {
	if r.electionTimer != nil {
		r.electionTimer.Reset(r.randomElectionTimeout())
	}
}
