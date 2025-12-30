// Package raft implements the core Raft consensus algorithm.
//
// election.go contains leader election related functionality including:
// - Election timeout randomization
// - Starting elections and requesting votes
// - Vote request/response handling
// - Leader state transitions
package raft

import (
	"log"
	"math/rand"
	"time"

	"github.com/salahayoub/anchor/api"
)

// randomElectionTimeout generates a randomized election timeout in the range
// [ElectionTimeout, 2*ElectionTimeout]. This randomization is crucial for
// preventing split votes: without it, all followers would timeout simultaneously
// after leader failure, all become candidates, and split the vote.
func (r *Raft) randomElectionTimeout() time.Duration {
	base := r.config.ElectionTimeout
	jitter := time.Duration(rand.Int63n(int64(base)))
	return base + jitter
}

// startElection initiates a new election by transitioning to Candidate state,
// incrementing the term, voting for self, and requesting votes from all peers.
// This method must be called from the main loop goroutine.
//
// PERSISTENCE ORDERING:
// This method ensures persistence happens BEFORE sending vote requests:
// 1. First, the incremented term is persisted to StableStore
// 2. Then, votedFor (self) is persisted to StableStore
// 3. Only after both persist successfully are vote requests sent to peers
// If any persistence fails, the method returns an error and no vote requests are sent.
func (r *Raft) startElection() error {
	// Transition to Candidate state
	r.state = Candidate

	// Increment currentTerm
	r.currentTerm++

	// Persist currentTerm before proceeding
	if err := r.stableStore.SetUint64(keyCurrentTerm, r.currentTerm); err != nil {
		return err
	}

	// Vote for self
	r.votedFor = r.config.ID

	// Persist votedFor before sending vote requests
	if err := r.stableStore.Set(keyVotedFor, []byte(r.votedFor)); err != nil {
		return err
	}

	// Initialize vote tracking for this election
	// Reset votes received and record self-vote
	r.votesReceived = make(map[string]bool)
	r.votesReceived[r.config.ID] = true // Vote for self
	r.electionTerm = r.currentTerm

	// Get last log info for vote request
	lastLogIndex, lastLogTerm, err := r.getLastLogInfo()
	if err != nil {
		return err
	}

	// Build vote request
	req := &api.VoteRequest{
		Term:         r.currentTerm,
		CandidateId:  []byte(r.config.ID),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// Send VoteRequest RPCs to all peers
	for _, peer := range r.config.Peers {
		go r.sendVoteRequest(peer, req)
	}

	// Check if we already have a majority (single-node cluster case)
	// This handles the case where there are no peers and self-vote is sufficient
	voters := r.getVotersUnlocked()
	majority := calculateQuorum(voters)
	voterVotes := 0
	for voterID := range r.votesReceived {
		if r.isVoterUnlocked(voterID) {
			voterVotes++
		}
	}
	if voterVotes >= majority {
		r.becomeLeader()
	}

	return nil
}

// sendVoteRequest sends a vote request to a peer and handles the response.
// This is called in a separate goroutine for each peer.
func (r *Raft) sendVoteRequest(peer string, req *api.VoteRequest) {
	resp, err := r.transport.SendRequestVote(peer, req)
	if err != nil {
		// Transport failure - will retry on next election
		return
	}

	r.handleVoteResponse(peer, resp)
}

// handleVoteResponse processes a vote response from a peer.
// Counts votes and transitions to leader when a majority of voters is achieved.
// Non-voters (learners) don't participate in elections - they receive log
// replication but can't vote, allowing safe cluster membership changes.
func (r *Raft) handleVoteResponse(peer string, resp *api.VoteResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If we're no longer a candidate, ignore the response
	if r.state != Candidate {
		return
	}

	// If response term is higher, step down to follower
	if resp.Term > r.currentTerm {
		if err := r.stepDownToFollower(resp.Term); err != nil {
			log.Printf("handleVoteResponse: failed to step down to follower: %v", err)
			return
		}
		return
	}

	// Ignore responses from old elections
	if resp.Term != r.electionTerm {
		return
	}

	// If vote was granted, record it
	if resp.VoteGranted {
		r.votesReceived[peer] = true

		// Check if we have a majority of voters
		// Only count votes from voters (non-voters don't participate in elections)
		voters := r.getVotersUnlocked()
		majority := calculateQuorum(voters)

		// Count votes received from voters only
		voterVotes := 0
		for voterID := range r.votesReceived {
			if r.isVoterUnlocked(voterID) {
				voterVotes++
			}
		}

		if voterVotes >= majority {
			// Transition to leader
			r.becomeLeader()
		}
	}
}

// handleVoteRequest processes an incoming VoteRequest RPC and returns a VoteResponse.
// It implements the Raft vote granting rules:
// - Deny if request term < currentTerm
// - Deny if already voted for different candidate in term
// - Deny if candidate's log is less up-to-date
// - Grant vote if all conditions pass
// - Update term if request term > currentTerm
//
// PERSISTENCE ORDERING:
// This method ensures all persistence happens BEFORE the response is returned:
// - If term changes, it is persisted via stepDownToFollower() before any other processing
// - If vote is granted, votedFor is persisted to StableStore before returning the response
// - On persistence failure, the vote is denied and state is not modified
func (r *Raft) handleVoteRequest(req *api.VoteRequest) *api.VoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &api.VoteResponse{
		Term:        r.currentTerm,
		VoteGranted: false,
	}

	// If request term > currentTerm, update term and transition to follower
	if req.Term > r.currentTerm {
		if err := r.stepDownToFollower(req.Term); err != nil {
			// On persistence failure, deny vote but return current term
			return resp
		}
		resp.Term = r.currentTerm
	}

	// Deny if request term < currentTerm
	if req.Term < r.currentTerm {
		return resp
	}

	candidateID := string(req.CandidateId)

	// Deny if already voted for different candidate in this term
	if r.votedFor != "" && r.votedFor != candidateID {
		return resp
	}

	// Check log up-to-dateness
	// Candidate's log is at least as up-to-date if:
	// - Candidate's last log term > our last log term, OR
	// - Terms are equal AND candidate's last log index >= our last log index
	lastLogIndex, lastLogTerm, err := r.getLastLogInfo()
	if err != nil {
		// On error reading log, deny vote
		return resp
	}

	if !r.isLogUpToDate(req.LastLogTerm, req.LastLogIndex, lastLogTerm, lastLogIndex) {
		return resp
	}

	// All conditions pass - grant vote
	// Persist votedFor before responding
	r.votedFor = candidateID
	if err := r.stableStore.Set(keyVotedFor, []byte(r.votedFor)); err != nil {
		// On persistence failure, revert and deny vote
		r.votedFor = ""
		return resp
	}

	resp.VoteGranted = true
	return resp
}

// becomeLeader transitions the node to leader state and initializes leader state.
func (r *Raft) becomeLeader() {
	r.state = Leader
	r.leaderID = r.config.ID

	// Get last log index for initializing nextIndex
	lastLogIndex, _, err := r.getLastLogInfo()
	if err != nil {
		// On error, use 0 as fallback
		lastLogIndex = 0
	}

	// Initialize nextIndex for each peer to lastLogIndex + 1
	// Initialize matchIndex for each peer to 0
	r.nextIndex = make(map[string]uint64)
	r.matchIndex = make(map[string]uint64)
	for _, peer := range r.config.Peers {
		r.nextIndex[peer] = lastLogIndex + 1
		r.matchIndex[peer] = 0
	}
}

// stepDownToFollower transitions the node to follower state with the given term.
// It persists the new term and clears votedFor.
// This method assumes the caller holds the mutex.
//
// PERSISTENCE ORDERING:
// This method ensures persistence happens BEFORE in-memory state is updated:
// 1. First, the new term is persisted to StableStore
// 2. Then, the in-memory currentTerm is updated
// 3. Next, votedFor is cleared and persisted
// 4. Finally, state is set to Follower
// If any persistence fails, the method returns an error and state remains unchanged.
func (r *Raft) stepDownToFollower(newTerm uint64) error {
	// Persist new term first
	if err := r.stableStore.SetUint64(keyCurrentTerm, newTerm); err != nil {
		return err
	}
	r.currentTerm = newTerm

	// Clear votedFor for new term and persist
	r.votedFor = ""
	if err := r.stableStore.Set(keyVotedFor, []byte{}); err != nil {
		return err
	}

	// Invalidate lease when stepping down from leadership
	if r.leaseState != nil {
		r.leaseState.Invalidate()
	}

	r.state = Follower
	return nil
}

// isLogUpToDate determines if the candidate's log is at least as up-to-date as ours.
// This check ensures the leader has all committed entries. A candidate with a
// stale log could overwrite committed entries if elected, violating safety.
// Comparison: higher term wins; if terms equal, longer log wins.
func (r *Raft) isLogUpToDate(candidateLastTerm, candidateLastIndex, ourLastTerm, ourLastIndex uint64) bool {
	if candidateLastTerm > ourLastTerm {
		return true
	}
	if candidateLastTerm == ourLastTerm && candidateLastIndex >= ourLastIndex {
		return true
	}
	return false
}
