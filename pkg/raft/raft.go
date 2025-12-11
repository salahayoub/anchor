// Package raft implements the core Raft consensus algorithm.
//
// # Thread Safety Guarantees
//
// The Raft struct is safe for concurrent use by multiple goroutines. Thread safety
// is achieved through a single goroutine running the main loop that handles all
// state transitions, RPC processing, and timer events via a select statement.
//
// Public methods that access state acquire the mutex for safe reads.
// State modifications only occur within the main loop goroutine.
//
// References:
package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/salahayoub/anchor/api"
	"github.com/salahayoub/anchor/pkg/transport"
)

// Error variables for Raft operations.
var (
	// ErrNotLeader is returned when a leader-only operation is attempted on a non-leader.
	ErrNotLeader = errors.New("node is not the leader")
	// ErrStopped is returned when operations are attempted on a stopped node.
	ErrStopped = errors.New("raft node is stopped")
	// ErrTimeout is returned when an operation times out.
	ErrTimeout = errors.New("operation timed out")
	// ErrLogInconsistent is returned when log consistency check fails.
	ErrLogInconsistent = errors.New("log consistency check failed")
	// ErrNoSnapshotStore is returned when snapshot operations are attempted without a snapshot store.
	ErrNoSnapshotStore = errors.New("no snapshot store configured")
	// ErrSnapshotFailed is returned when a snapshot operation fails.
	ErrSnapshotFailed = errors.New("snapshot operation failed")
)

// Stable store keys for persistent state.
var (
	keyCurrentTerm = []byte("currentTerm")
	keyVotedFor    = []byte("votedFor")
)

// NodeState represents the current role of a Raft node.
type NodeState int

const (
	// Follower is a passive node that responds to RPCs from leaders and candidates.
	Follower NodeState = iota
	// Candidate is a node actively seeking votes to become leader.
	Candidate
	// Leader is the node responsible for handling client requests and replicating logs.
	Leader
)

// String returns a human-readable representation of the NodeState.
func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Config holds the configuration for a Raft node.
type Config struct {
	// ID is the unique identifier for this node.
	ID string
	// Peers contains the addresses of all peer nodes in the cluster.
	Peers []string
	// ElectionTimeout is the base election timeout. Actual timeout is randomized
	// in the range [ElectionTimeout, 2*ElectionTimeout].
	ElectionTimeout time.Duration
	// HeartbeatTimeout is the interval at which the leader sends heartbeats.
	HeartbeatTimeout time.Duration
	// SnapshotThreshold is the number of log entries after which automatic
	// snapshot is triggered. 0 disables automatic snapshots.
	SnapshotThreshold uint64
	// SnapshotChunkSize is the size of chunks in bytes when sending snapshots
	// to followers via InstallSnapshot RPC. Default: 1MB (1048576 bytes).
	SnapshotChunkSize int
}

// DefaultSnapshotChunkSize is the default chunk size for InstallSnapshot RPC (1MB).
const DefaultSnapshotChunkSize = 1024 * 1024

// LogStore interface for persistent log storage.
type LogStore interface {
	// FirstIndex returns the first index written to the log store.
	FirstIndex() (uint64, error)
	// LastIndex returns the last index written to the log store.
	LastIndex() (uint64, error)
	// GetLog retrieves a log entry at the specified index.
	GetLog(index uint64) (*api.LogEntry, error)
	// StoreLogs stores multiple log entries.
	StoreLogs(logs []*api.LogEntry) error
	// DeleteRange removes all log entries within the specified range inclusive.
	DeleteRange(min, max uint64) error
}

// StableStore interface for persistent state storage.
type StableStore interface {
	// Get retrieves a value by key.
	Get(key []byte) ([]byte, error)
	// Set stores a key-value pair.
	Set(key []byte, val []byte) error
	// GetUint64 retrieves a uint64 value by key.
	GetUint64(key []byte) (uint64, error)
	// SetUint64 stores a uint64 value.
	SetUint64(key []byte, val uint64) error
}

// StateMachine interface for application state.
type StateMachine interface {
	// Apply takes command bytes and modifies state, returning any result.
	Apply(logBytes []byte) interface{}
	// Snapshot returns a reader containing serialized state.
	Snapshot() (io.ReadCloser, error)
	// Restore replaces current state with data from the reader.
	Restore(rc io.ReadCloser) error
}

// pendingSnapshotState tracks the state of an in-progress snapshot installation
// from a leader via InstallSnapshot RPC.
type pendingSnapshotState struct {
	meta       *SnapshotMeta // Metadata for the snapshot being received
	file       *os.File      // Temporary file for writing snapshot data
	bytesRecvd int64         // Number of bytes received so far
}

// Raft implements the core consensus algorithm.
// It coordinates leader election, log replication, and commit processing.
type Raft struct {
	// Persistent state (persisted to StableStore before responding to RPCs)
	currentTerm uint64
	votedFor    string

	// Volatile state on all servers
	state       NodeState
	commitIndex uint64
	lastApplied uint64

	// Volatile state on leaders (reinitialized after election)
	nextIndex  map[string]uint64 // For each peer: next log index to send
	matchIndex map[string]uint64 // For each peer: highest log index replicated

	// Leader tracking
	leaderID string

	// Cluster membership configuration
	// Tracks current cluster members and their voting status
	clusterConfig *ClusterConfig

	// Snapshot state
	snapshotStore     SnapshotStore // Persistent storage for snapshots
	lastSnapshotIndex uint64        // lastIncludedIndex of most recent snapshot
	lastSnapshotTerm  uint64        // lastIncludedTerm of most recent snapshot
	snapshotting      bool          // True when automatic snapshot is in progress

	// Pending snapshot installation (follower side)
	pendingSnapshot *pendingSnapshotState

	// Dependencies
	logStore     LogStore
	stableStore  StableStore
	stateMachine StateMachine
	transport    transport.Transport

	// Configuration
	config Config

	// Channels and timers
	rpcChan         <-chan transport.RPC
	stopChan        chan struct{}
	doneChan        chan struct{} // Signals when main loop has exited
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	// Election state
	votesReceived map[string]bool // Tracks votes received in current election
	electionTerm  uint64          // Term of the current election

	// Synchronization
	mu      sync.RWMutex
	running bool
}

// NewRaft creates a new Raft node with the given configuration.
// It loads persisted state from StableStore and initializes the node as a Follower.
// If snapshotStore is provided, it loads existing snapshot metadata on startup
// and restores the state machine from the snapshot if one exists.
func NewRaft(config Config, logStore LogStore, stableStore StableStore,
	stateMachine StateMachine, trans transport.Transport, snapshotStore SnapshotStore) (*Raft, error) {

	// Load persisted currentTerm
	currentTerm, err := stableStore.GetUint64(keyCurrentTerm)
	if err != nil {
		return nil, err
	}

	// Load persisted votedFor
	votedForBytes, err := stableStore.Get(keyVotedFor)
	if err != nil {
		return nil, err
	}
	votedFor := string(votedForBytes)

	// Initialize cluster config from config.Peers
	// Self is always a Voter, peers start as Voters for bootstrap
	clusterConfig := &ClusterConfig{
		Members: make([]ClusterMember, 0, len(config.Peers)+1),
	}
	// Add self as a Voter
	clusterConfig.Members = append(clusterConfig.Members, ClusterMember{
		ID:      config.ID,
		Address: "", // Self address not needed
		State:   Voter,
	})
	// Add peers as Voters (bootstrap configuration)
	for _, peer := range config.Peers {
		clusterConfig.Members = append(clusterConfig.Members, ClusterMember{
			ID:      peer,
			Address: peer,
			State:   Voter,
		})
	}

	// Initialize snapshot state and restore from snapshot if available
	var lastSnapshotIndex, lastSnapshotTerm uint64
	var commitIndex, lastApplied uint64
	if snapshotStore != nil {
		meta, err := snapshotStore.GetMeta()
		if err == nil {
			// Snapshot exists, restore from it- check for existing snapshots on startup
			lastSnapshotIndex = meta.LastIncludedIndex
			lastSnapshotTerm = meta.LastIncludedTerm

			// Open snapshot to get data for state machine restore- restore state machine using StateMachine.Restore()
			_, reader, err := snapshotStore.Open()
			if err != nil {
				return nil, fmt.Errorf("failed to open snapshot for restore: %w", err)
			}

			// Restore state machine from snapshot data
			if err := stateMachine.Restore(reader); err != nil {
				return nil, fmt.Errorf("failed to restore state machine from snapshot: %w", err)
			}

			// Set lastApplied and commitIndex to lastIncludedIndex- set lastApplied and commitIndex to snapshot's lastIncludedIndex
			commitIndex = meta.LastIncludedIndex
			lastApplied = meta.LastIncludedIndex

			// Restore cluster configuration from snapshot metadata- restore cluster configuration from snapshot metadata
			if meta.Configuration != nil {
				clusterConfig = meta.Configuration
			}
		} else if err != ErrNoSnapshot {
			// Unexpected error reading snapshot metadata- return error if snapshot is corrupted or invalid
			return nil, fmt.Errorf("failed to read snapshot metadata: %w", err)
		}
		// ErrNoSnapshot is expected when no snapshot exists, continue with zeros
	}

	r := &Raft{
		// Persistent state loaded from storage
		currentTerm: currentTerm,
		votedFor:    votedFor,

		// Volatile state initialized (may be updated from snapshot)
		state:       Follower,
		commitIndex: commitIndex,
		lastApplied: lastApplied,

		// Leader state (initialized when becoming leader)
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),

		// Cluster membership (may be restored from snapshot)
		clusterConfig: clusterConfig,

		// Snapshot state
		snapshotStore:     snapshotStore,
		lastSnapshotIndex: lastSnapshotIndex,
		lastSnapshotTerm:  lastSnapshotTerm,

		// Dependencies
		logStore:     logStore,
		stableStore:  stableStore,
		stateMachine: stateMachine,
		transport:    trans,

		// Configuration
		config: config,

		// Channels
		rpcChan:  trans.Consumer(),
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),

		// Election state
		votesReceived: make(map[string]bool),
		electionTerm:  0,

		// Not running until Start() is called
		running: false,
	}

	// Reconstruct cluster membership from committed LOG_CONFIG entries
	// This ensures membership survives restarts
	// Note: This may override snapshot config if there are newer config entries in the log
	if err := r.reconstructConfigFromLog(); err != nil {
		return nil, err
	}

	return r, nil
}

// State returns the current node state (Follower, Candidate, Leader).
func (r *Raft) State() NodeState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// Leader returns the current known leader ID (empty if unknown).
func (r *Raft) Leader() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaderID
}

// CurrentTerm returns the current term of the node.
func (r *Raft) CurrentTerm() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm
}

// VotedFor returns the candidate ID that received vote in current term.
func (r *Raft) VotedFor() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.votedFor
}

// NextIndex returns the nextIndex map for testing purposes.
// Returns a copy to prevent external modification.
func (r *Raft) NextIndex() map[string]uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make(map[string]uint64)
	for k, v := range r.nextIndex {
		result[k] = v
	}
	return result
}

// MatchIndex returns the matchIndex map for testing purposes.
// Returns a copy to prevent external modification.
func (r *Raft) MatchIndex() map[string]uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make(map[string]uint64)
	for k, v := range r.matchIndex {
		result[k] = v
	}
	return result
}

// VotesReceived returns the number of votes received in the current election.
// For testing purposes.
func (r *Raft) VotesReceived() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.votesReceived)
}

// LastSnapshotIndex returns the lastIncludedIndex of the most recent snapshot.
// For testing purposes.
func (r *Raft) LastSnapshotIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastSnapshotIndex
}

// LastSnapshotTerm returns the lastIncludedTerm of the most recent snapshot.
// For testing purposes.
func (r *Raft) LastSnapshotTerm() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastSnapshotTerm
}

// GetConfiguration returns the current cluster configuration.
// Returns a copy to prevent external modification.
func (r *Raft) GetConfiguration() ClusterConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.clusterConfig == nil {
		return ClusterConfig{}
	}
	// Return a deep copy
	config := ClusterConfig{
		Members: make([]ClusterMember, len(r.clusterConfig.Members)),
	}
	copy(config.Members, r.clusterConfig.Members)
	return config
}

// IsVoter returns whether the specified node is a voter in the current configuration.
func (r *Raft) IsVoter(nodeID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.clusterConfig == nil {
		return false
	}
	for _, member := range r.clusterConfig.Members {
		if member.ID == nodeID {
			return member.State == Voter
		}
	}
	return false
}

// GetVoters returns a list of all voter node IDs in the current configuration.
func (r *Raft) GetVoters() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.clusterConfig == nil {
		return nil
	}
	voters := make([]string, 0)
	for _, member := range r.clusterConfig.Members {
		if member.State == Voter {
			voters = append(voters, member.ID)
		}
	}
	return voters
}

// GetNonVoters returns a list of all non-voter node IDs in the current configuration.
func (r *Raft) GetNonVoters() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.clusterConfig == nil {
		return nil
	}
	nonVoters := make([]string, 0)
	for _, member := range r.clusterConfig.Members {
		if member.State == NonVoter {
			nonVoters = append(nonVoters, member.ID)
		}
	}
	return nonVoters
}

// calculateQuorum returns the minimum number of nodes needed for a majority
// given a list of voters. Quorum = (len(voters) / 2) + 1
func calculateQuorum(voters []string) int {
	return (len(voters) / 2) + 1
}

// getVotersUnlocked returns a list of all voter node IDs without acquiring the lock.
// This method assumes the caller holds the mutex.
func (r *Raft) getVotersUnlocked() []string {
	if r.clusterConfig == nil {
		return nil
	}
	voters := make([]string, 0)
	for _, member := range r.clusterConfig.Members {
		if member.State == Voter {
			voters = append(voters, member.ID)
		}
	}
	return voters
}

// isVoterUnlocked returns whether the specified node is a voter without acquiring the lock.
// This method assumes the caller holds the mutex.
func (r *Raft) isVoterUnlocked(nodeID string) bool {
	if r.clusterConfig == nil {
		return false
	}
	for _, member := range r.clusterConfig.Members {
		if member.ID == nodeID {
			return member.State == Voter
		}
	}
	return false
}

// randomElectionTimeout generates a randomized election timeout in the range
// [ElectionTimeout, 2*ElectionTimeout]. This randomization helps prevent
// split votes by ensuring nodes don't all timeout at the same time.
func (r *Raft) randomElectionTimeout() time.Duration {
	base := r.config.ElectionTimeout
	// Generate random duration in range [base, 2*base]
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
//
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
// It counts votes and transitions to leader when a majority of voters is achieved.
// Only voters are counted in election quorum calculations.
func (r *Raft) handleVoteResponse(peer string, resp *api.VoteResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If we're no longer a candidate, ignore the response
	if r.state != Candidate {
		return
	}

	// If response term is higher, step down to follower
	if resp.Term > r.currentTerm {
		_ = r.stepDownToFollower(resp.Term)
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
//
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

	r.state = Follower
	return nil
}

// isLogUpToDate determines if the candidate's log is at least as up-to-date as ours.
// The candidate's log is more up-to-date if:
// - Its last log term is greater than ours, OR
// - Terms are equal AND its last log index is >= ours
func (r *Raft) isLogUpToDate(candidateLastTerm, candidateLastIndex, ourLastTerm, ourLastIndex uint64) bool {
	if candidateLastTerm > ourLastTerm {
		return true
	}
	if candidateLastTerm == ourLastTerm && candidateLastIndex >= ourLastIndex {
		return true
	}
	return false
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
//
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

			// Append new entries (- persist before responding)
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

// resetElectionTimer resets the election timer to a new random timeout.
// This method assumes the caller holds the mutex.
func (r *Raft) resetElectionTimer() {
	if r.electionTimer != nil {
		r.electionTimer.Reset(r.randomElectionTimeout())
	}
}

// handleInstallSnapshot processes an incoming InstallSnapshot RPC and returns an InstallSnapshotResponse.
// It implements the Raft snapshot installation rules:
// - Reject if request term < currentTerm
// - Reset election timer on valid request
// - If offset == 0, start new pending snapshot
// - Write data chunk at specified offset
// - If done == true, finalize snapshot installation
//
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
// It moves the temp file to the snapshot location, restores the state machine,
// discards the entire log, and updates indices and configuration.
// This method assumes the caller holds the mutex.
//
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

	// Move temp file to snapshot location via SnapshotStore- atomically move the temporary file to the snapshot location
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

	// Restore state machine from snapshot data- restore the state machine using StateMachine.Restore()
	snapshotFile, err := os.Open(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to open snapshot for restore: %w", err)
	}
	defer snapshotFile.Close()

	if err := r.stateMachine.Restore(snapshotFile); err != nil {
		return fmt.Errorf("failed to restore state machine: %w", err)
	}

	// Discard entire log (DeleteRange from first to last)- discard the entire log
	firstIndex, err := r.logStore.FirstIndex()
	if err == nil && firstIndex > 0 {
		lastIndex, err := r.logStore.LastIndex()
		if err == nil && lastIndex >= firstIndex {
			if err := r.logStore.DeleteRange(firstIndex, lastIndex); err != nil {
				// Log deletion failed, but snapshot is installed
				// Continue anyway
			}
		}
	}

	// Update compacted index if the log store supports it
	if compactable, ok := r.logStore.(CompactableLogStore); ok {
		_ = compactable.SetCompactedIndex(meta.LastIncludedIndex)
	}

	// Set lastApplied and commitIndex to lastIncludedIndex- set lastApplied and commitIndex to lastIncludedIndex
	r.lastApplied = meta.LastIncludedIndex
	r.commitIndex = meta.LastIncludedIndex

	// Update snapshot state
	r.lastSnapshotIndex = meta.LastIncludedIndex
	r.lastSnapshotTerm = meta.LastIncludedTerm

	// Update cluster configuration from snapshot- update the cluster configuration from the snapshot
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

// CommitIndex returns the current commit index for testing purposes.
func (r *Raft) CommitIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.commitIndex
}

// LastApplied returns the last applied index for testing purposes.
func (r *Raft) LastApplied() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastApplied
}

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

	// Check if peer needs a snapshot instead of AppendEntries- if nextIndex < firstLogIndex, send InstallSnapshot
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
		_ = r.stepDownToFollower(resp.Term)
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

// checkAndPromoteNonVoter checks if a peer is a NonVoter that has caught up
// and should be promoted to Voter status.
// A NonVoter is promoted when its matchIndex >= leader's commitIndex.
// This method assumes the caller holds the mutex.
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

	// Check if NonVoter's matchIndex >= commitIndex- promote when matchIndex reaches commitIndex
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

// advanceCommitIndex calculates the majority matchIndex among voters and advances commitIndex
// if the entry's term equals the current term.
// Only voters are counted in commit quorum calculations.
// This method assumes the caller holds the mutex.
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

	// Only commit entries from the current term (Raft safety property)- advance commitIndex only if entry's term equals currentTerm
	entry, err := r.logStore.GetLog(majorityIndex)
	if err != nil {
		return
	}

	if entry.Term != r.currentTerm {
		// Cannot commit entries from previous terms directly
		// They will be committed indirectly when a current-term entry is committed
		return
	}

	// Advance commitIndex
	r.commitIndex = majorityIndex

	// Apply newly committed entries
	r.applyEntries()
}

// applyEntries applies all committed but not yet applied entries to the state machine.
// It applies entries from lastApplied + 1 to commitIndex in strictly increasing order.
// This method assumes the caller holds the mutex.
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

// maybeSnapshot checks if the log size exceeds the threshold and triggers
// an automatic snapshot in a background goroutine.
// This method assumes the caller holds the mutex.
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

	// Trigger snapshot in background goroutine to avoid blocking- continue processing client requests and RPCs without blocking
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

	// Compact the log after successful snapshot- delete all log entries up to and including lastSnapshotIndex
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
// This ensures cluster membership survives node restarts.
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

	// Scan all log entries in order and apply config entries- apply in log order to maintain consistency
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

// ApplyEntries is a public method to trigger entry application.
// This is useful for testing and for the main loop to call.
func (r *Raft) ApplyEntries() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applyEntries()
}

// SetCommitIndex sets the commit index directly for testing purposes.
// This should only be used in tests.
func (r *Raft) SetCommitIndex(index uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commitIndex = index
}

// Start begins the Raft consensus loop.
// It initializes the election timer and starts the main loop goroutine.
func (r *Raft) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return nil // Already running
	}

	// Replay committed log entries to rebuild state machine
	// This handles the case where entries were committed but not snapshotted
	if err := r.replayLogEntries(); err != nil {
		return fmt.Errorf("failed to replay log entries: %w", err)
	}

	// Initialize election timer with random timeout
	r.electionTimer = time.NewTimer(r.randomElectionTimeout())

	// Initialize heartbeat ticker (only used when leader)
	r.heartbeatTicker = time.NewTicker(r.config.HeartbeatTimeout)

	r.running = true

	// Start main loop goroutine
	go r.run()

	return nil
}

// replayLogEntries replays all committed log entries from lastApplied+1 to the last log index.
// This rebuilds the state machine after a restart when entries were committed but not snapshotted.
// This method assumes the caller holds the mutex.
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

// Stop gracefully shuts down the Raft node.
// It signals the stop channel and waits for the main loop to exit.
func (r *Raft) Stop() error {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return nil
	}
	r.running = false
	r.mu.Unlock()

	// Signal stop channel
	close(r.stopChan)

	// Wait for main loop to exit
	<-r.doneChan

	return nil
}

// run is the main loop that handles all state transitions, RPC processing,
// and timer events through a select statement.
// This method runs in its own goroutine and ensures thread-safe operation.
func (r *Raft) run() {
	defer close(r.doneChan) // Signal that main loop has exited

	for {
		select {
		case <-r.stopChan:
			// Clean up resources and exit
			r.electionTimer.Stop()
			r.heartbeatTicker.Stop()
			return

		case <-r.electionTimer.C:
			// Election timeout expired
			r.mu.Lock()
			if r.state != Leader {
				// Follower or Candidate: start a new election
				_ = r.startElection() // Errors are logged internally, continue regardless
			}
			// Reset election timer
			r.electionTimer.Reset(r.randomElectionTimeout())
			r.mu.Unlock()

		case <-r.heartbeatTicker.C:
			// Heartbeat interval elapsed (leader only)
			r.mu.RLock()
			isLeader := r.state == Leader
			peers := r.config.Peers
			r.mu.RUnlock()

			if isLeader {
				// Send AppendEntries (heartbeats) to all peers
				for _, peer := range peers {
					r.sendAppendEntries(peer)
				}
			}

		case rpc := <-r.rpcChan:
			// Handle incoming RPC
			r.handleRPC(rpc)
		}
	}
}

// handleRPC processes an incoming RPC request and sends the response.
// It dispatches to the appropriate handler based on request type.
func (r *Raft) handleRPC(rpc transport.RPC) {
	var resp transport.RPCResponse

	switch req := rpc.Request.(type) {
	case *api.VoteRequest:
		// Check for higher term before processing
		r.mu.Lock()
		if req.Term > r.currentTerm {
			// Update term and transition to follower
			if err := r.stepDownToFollower(req.Term); err != nil {
				r.mu.Unlock()
				resp.Error = err
				rpc.RespChan <- resp
				return
			}
		}
		r.mu.Unlock()

		// Process vote request
		voteResp := r.handleVoteRequest(req)
		resp.Response = voteResp

	case *api.AppendEntriesRequest:
		// Check for higher term and handle candidate transition
		r.mu.Lock()
		if req.Term > r.currentTerm {
			// Update term and transition to follower
			if err := r.stepDownToFollower(req.Term); err != nil {
				r.mu.Unlock()
				resp.Error = err
				rpc.RespChan <- resp
				return
			}
		} else if req.Term >= r.currentTerm && r.state == Candidate {
			// Candidate receives AppendEntries with term >= currentTerm
			// Transition to follower
			r.state = Follower
			r.leaderID = string(req.LeaderId)
		}
		r.mu.Unlock()

		// Process append entries request
		appendResp := r.handleAppendEntries(req)
		resp.Response = appendResp

	case *api.InstallSnapshotRequest:
		// Process InstallSnapshot request
		installResp := r.handleInstallSnapshot(req)
		resp.Response = installResp

	case *api.JoinClusterRequest:
		// Handle join cluster request
		joinResp := r.handleJoinCluster(req)
		resp.Response = joinResp

	case *api.RemoveServerRequest:
		// Handle remove server request
		removeResp := r.handleRemoveServer(req)
		resp.Response = removeResp

	default:
		resp.Error = errors.New("unknown RPC type")
	}

	// Send response back through the channel
	rpc.RespChan <- resp
}

// JoinCluster handles a request from a new node to join the cluster.
// It creates a LOG_CONFIG entry with the new node as NonVoter and waits for commit.
// This method is idempotent - if the node already exists, it returns success.
func (r *Raft) JoinCluster(nodeID, addr string) error {
	r.mu.Lock()

	// Verify node is leader, return error with leader hint if not
	if r.state != Leader {
		leaderHint := r.leaderID
		r.mu.Unlock()
		return &NotLeaderError{LeaderHint: leaderHint}
	}

	// Check if running
	if !r.running {
		r.mu.Unlock()
		return ErrStopped
	}

	// Check if nodeID already exists (idempotency)
	for _, member := range r.clusterConfig.Members {
		if member.ID == nodeID {
			// Node already exists, return success without creating duplicate entry
			r.mu.Unlock()
			return nil
		}
	}

	// Create new cluster config with the new node as NonVoter
	newMembers := make([]ClusterMember, len(r.clusterConfig.Members), len(r.clusterConfig.Members)+1)
	copy(newMembers, r.clusterConfig.Members)
	newMembers = append(newMembers, ClusterMember{
		ID:      nodeID,
		Address: addr,
		State:   NonVoter,
	})
	newConfig := &ClusterConfig{Members: newMembers}

	// Serialize the new config
	configData, err := SerializeConfig(newConfig)
	if err != nil {
		r.mu.Unlock()
		return err
	}

	// Get next log index
	lastIndex, err := r.logStore.LastIndex()
	if err != nil {
		r.mu.Unlock()
		return err
	}
	newIndex := lastIndex + 1

	// Create LOG_CONFIG entry
	entry := &api.LogEntry{
		Index: newIndex,
		Term:  r.currentTerm,
		Type:  api.LogType_LOG_CONFIGURATION,
		Data:  configData,
	}

	// Append entry to log
	if err := r.logStore.StoreLogs([]*api.LogEntry{entry}); err != nil {
		r.mu.Unlock()
		return err
	}

	// Initialize nextIndex/matchIndex for new peer
	r.nextIndex[addr] = newIndex + 1
	r.matchIndex[addr] = 0

	r.mu.Unlock()

	// Wait for commit (with timeout)
	timeout := 10 * time.Second
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

// NotLeaderError is returned when a leader-only operation is attempted on a non-leader.
// It includes a hint about the current leader's address.
type NotLeaderError struct {
	LeaderHint string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderHint != "" {
		return "node is not the leader; leader is " + e.LeaderHint
	}
	return "node is not the leader"
}

// Is implements errors.Is for NotLeaderError.
func (e *NotLeaderError) Is(target error) bool {
	return target == ErrNotLeader
}

// handleJoinCluster processes an incoming JoinCluster RPC and returns a JoinClusterResponse.
// It calls the JoinCluster method and converts the result to a response.
func (r *Raft) handleJoinCluster(req *api.JoinClusterRequest) *api.JoinClusterResponse {
	err := r.JoinCluster(req.NodeId, req.Address)
	if err != nil {
		// Check if it's a NotLeaderError to provide leader hint
		if notLeaderErr, ok := err.(*NotLeaderError); ok {
			return &api.JoinClusterResponse{
				Success:    false,
				LeaderHint: notLeaderErr.LeaderHint,
				Error:      notLeaderErr.Error(),
			}
		}
		return &api.JoinClusterResponse{
			Success: false,
			Error:   err.Error(),
		}
	}
	return &api.JoinClusterResponse{
		Success: true,
	}
}

// ErrNodeNotFound is returned when attempting to remove a node that doesn't exist.
var ErrNodeNotFound = errors.New("node not found in cluster configuration")

// RemoveServer handles a request to remove a node from the cluster.
// It creates a LOG_CONFIG entry without the removed node and waits for commit.
// If the leader removes itself, it steps down after the entry is committed.
func (r *Raft) RemoveServer(nodeID string) error {
	r.mu.Lock()

	// Verify node is leader, return error with leader hint if not
	if r.state != Leader {
		leaderHint := r.leaderID
		r.mu.Unlock()
		return &NotLeaderError{LeaderHint: leaderHint}
	}

	// Check if running
	if !r.running {
		r.mu.Unlock()
		return ErrStopped
	}

	// Check if nodeID exists in the cluster configuration
	nodeFound := false
	removingSelf := nodeID == r.config.ID
	for _, member := range r.clusterConfig.Members {
		if member.ID == nodeID {
			nodeFound = true
			break
		}
	}

	if !nodeFound {
		r.mu.Unlock()
		return ErrNodeNotFound
	}

	// Create new cluster config without the removed node
	newMembers := make([]ClusterMember, 0, len(r.clusterConfig.Members)-1)
	for _, member := range r.clusterConfig.Members {
		if member.ID != nodeID {
			newMembers = append(newMembers, member)
		}
	}
	newConfig := &ClusterConfig{Members: newMembers}

	// Serialize the new config
	configData, err := SerializeConfig(newConfig)
	if err != nil {
		r.mu.Unlock()
		return err
	}

	// Get next log index
	lastIndex, err := r.logStore.LastIndex()
	if err != nil {
		r.mu.Unlock()
		return err
	}
	newIndex := lastIndex + 1

	// Create LOG_CONFIG entry
	entry := &api.LogEntry{
		Index: newIndex,
		Term:  r.currentTerm,
		Type:  api.LogType_LOG_CONFIGURATION,
		Data:  configData,
	}

	// Append entry to log
	if err := r.logStore.StoreLogs([]*api.LogEntry{entry}); err != nil {
		r.mu.Unlock()
		return err
	}

	r.mu.Unlock()

	// Wait for commit (with timeout)
	timeout := 10 * time.Second
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
				// If removing self, step down after commit
				if removingSelf {
					r.mu.Lock()
					r.state = Follower
					r.leaderID = ""
					r.mu.Unlock()
				}
				return nil
			}
			if !isLeader {
				return ErrNotLeader
			}
		}
	}
}

// handleRemoveServer processes an incoming RemoveServer RPC and returns a RemoveServerResponse.
// It calls the RemoveServer method and converts the result to a response.
func (r *Raft) handleRemoveServer(req *api.RemoveServerRequest) *api.RemoveServerResponse {
	err := r.RemoveServer(req.NodeId)
	if err != nil {
		// Check if it's a NotLeaderError to provide leader hint
		if notLeaderErr, ok := err.(*NotLeaderError); ok {
			return &api.RemoveServerResponse{
				Success:    false,
				LeaderHint: notLeaderErr.LeaderHint,
				Error:      notLeaderErr.Error(),
			}
		}
		return &api.RemoveServerResponse{
			Success: false,
			Error:   err.Error(),
		}
	}
	return &api.RemoveServerResponse{
		Success: true,
	}
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

// CompactableLogStore extends LogStore with compaction tracking capabilities.
type CompactableLogStore interface {
	LogStore
	// SetCompactedIndex sets the compacted index for tracking compaction state.
	SetCompactedIndex(index uint64) error
}

// compactLog deletes log entries from FirstIndex to lastSnapshotIndex.
// This should be called after a successful snapshot to reclaim disk space.
// This method assumes the caller holds the mutex.
func (r *Raft) compactLog() error {
	// If no snapshot has been taken, nothing to compact
	if r.lastSnapshotIndex == 0 {
		return nil
	}

	// Get the first index in the log
	firstIndex, err := r.logStore.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get first log index: %w", err)
	}

	// If log is empty, just update the compacted index
	if firstIndex == 0 {
		// Update compacted index if the log store supports it
		if compactable, ok := r.logStore.(CompactableLogStore); ok {
			if err := compactable.SetCompactedIndex(r.lastSnapshotIndex); err != nil {
				return fmt.Errorf("failed to set compacted index: %w", err)
			}
		}
		return nil
	}

	// Don't compact if firstIndex is already past the snapshot
	if firstIndex > r.lastSnapshotIndex {
		return nil
	}

	// Delete log entries from firstIndex to lastSnapshotIndex (inclusive)
	if err := r.logStore.DeleteRange(firstIndex, r.lastSnapshotIndex); err != nil {
		return fmt.Errorf("failed to delete log range [%d, %d]: %w", firstIndex, r.lastSnapshotIndex, err)
	}

	// Update compacted index if the log store supports it
	if compactable, ok := r.logStore.(CompactableLogStore); ok {
		if err := compactable.SetCompactedIndex(r.lastSnapshotIndex); err != nil {
			return fmt.Errorf("failed to set compacted index: %w", err)
		}
	}

	return nil
}

// CompactLog is a public method to trigger log compaction after a snapshot.
// This deletes all log entries up to and including lastSnapshotIndex.
func (r *Raft) CompactLog() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.compactLog()
}

// handleInstallSnapshotResponse processes the response from an InstallSnapshot RPC.
// On success with done=true, it updates nextIndex to lastIncludedIndex + 1.
// On failure (higher term), it steps down to follower.
//
func (r *Raft) handleInstallSnapshotResponse(peer string, meta *SnapshotMeta, resp *api.InstallSnapshotResponse, done bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If we're no longer the leader, ignore the response
	if r.state != Leader {
		return
	}

	// If response term is higher, step down to follower
	if resp.Term > r.currentTerm {
		_ = r.stepDownToFollower(resp.Term)
		return
	}

	// On success with done=true, update nextIndex to lastIncludedIndex + 1- update nextIndex after successful snapshot installation
	if done {
		r.nextIndex[peer] = meta.LastIncludedIndex + 1
		r.matchIndex[peer] = meta.LastIncludedIndex
	}
}

// sendInstallSnapshot sends a snapshot to a lagging follower via InstallSnapshot RPC.
// It reads the snapshot from the SnapshotStore and sends it in chunks of SnapshotChunkSize.
// Each chunk is sent with incrementing offsets, and the final chunk has done=true.
// The method waits for a response before sending the next chunk.
//
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

	// Send snapshot in chunks- send in multiple chunks with incrementing offsets
	var offset uint64 = 0
	buf := make([]byte, chunkSize)
	var lastChunkSent bool = false

	for !lastChunkSent {
		// Read a chunk of data
		n, readErr := reader.Read(buf)

		// Determine if this is the final chunk- set done=true on final chunk
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

		// Build InstallSnapshot request- include all required fields
		req := &api.InstallSnapshotRequest{
			Term:              currentTerm,
			LeaderId:          []byte(leaderID),
			LastIncludedIndex: meta.LastIncludedIndex,
			LastIncludedTerm:  meta.LastIncludedTerm,
			Offset:            offset,
			Data:              buf[:n],
			Done:              done,
		}

		// Send the chunk and wait for response- wait for response before sending next chunk
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
			_ = r.stepDownToFollower(resp.Term)
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

// Snapshot takes a snapshot of the current state machine state and persists it.
// This can be called on any node (leader or follower) to take a local snapshot.
// The snapshot includes the state machine data, lastApplied index and term,
// and the current cluster configuration.
//
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

	// Capture current state for snapshot- snapshot can be taken on any node
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

	// Copy cluster configuration- include cluster configuration in snapshot
	var config *ClusterConfig
	if r.clusterConfig != nil {
		config = &ClusterConfig{
			Members: make([]ClusterMember, len(r.clusterConfig.Members)),
		}
		copy(config.Members, r.clusterConfig.Members)
	}

	r.mu.Unlock()

	// Capture state machine state- capture state machine state using StateMachine.Snapshot()
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

	// Create snapshot metadata- persist snapshot data along with metadata
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

	// Update snapshot state- return snapshot metadata to caller
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
