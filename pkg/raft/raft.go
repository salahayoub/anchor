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
// File Organization:
// - raft.go: Core types, interfaces, Raft struct, NewRaft, public getters, Start/Stop, main loop
// - election.go: Leader election (startElection, handleVoteRequest/Response, becomeLeader, stepDownToFollower)
// - replication.go: Log replication (sendAppendEntries, handleAppendEntries, advanceCommitIndex)
// - snapshot_ops.go: Snapshot operations (handleInstallSnapshot, sendInstallSnapshot, Snapshot)
// - apply.go: Entry application (applyEntries, applyConfigEntry, Apply)
package raft

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/salahayoub/anchor/api"
	"github.com/salahayoub/anchor/pkg/transport"
)

// Sentinel errors for Raft operations. Using sentinel errors enables callers
// to use errors.Is() for reliable error handling even when errors are wrapped.
var (
	ErrNotLeader       = errors.New("node is not the leader")
	ErrStopped         = errors.New("raft node is stopped")
	ErrTimeout         = errors.New("operation timed out")
	ErrLogInconsistent = errors.New("log consistency check failed")
	ErrNoSnapshotStore = errors.New("no snapshot store configured")
	ErrSnapshotFailed  = errors.New("snapshot operation failed")
)

// Keys for persisting Raft state that must survive restarts. Per the Raft paper,
// currentTerm and votedFor must be persisted before responding to any RPC to
// prevent a node from voting twice in the same term after a crash.
var (
	keyCurrentTerm = []byte("currentTerm")
	keyVotedFor    = []byte("votedFor")
)

// NodeState represents the current role of a Raft node in the consensus protocol.
// Transitions: Follower → Candidate (on election timeout) → Leader (on majority vote)
//
//	Any state → Follower (on discovering higher term)
type NodeState int

const (
	Follower  NodeState = iota // Passive: responds to RPCs, doesn't initiate
	Candidate                  // Actively seeking votes to become leader
	Leader                     // Handles client requests, replicates logs to followers
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
	ID    string   // Unique identifier for this node (must be stable across restarts)
	Peers []string // Network addresses of peer nodes (excluding self)

	// ElectionTimeout is the base timeout before starting an election.
	// Randomized to [ElectionTimeout, 2*ElectionTimeout] to prevent split votes
	// when multiple followers timeout simultaneously after leader failure.
	ElectionTimeout time.Duration

	// HeartbeatTimeout controls how often the leader sends empty AppendEntries.
	// Must be significantly less than ElectionTimeout to prevent unnecessary elections.
	HeartbeatTimeout time.Duration

	// SnapshotThreshold triggers automatic snapshots when log grows beyond
	// this many entries. Snapshots reduce memory usage and speed up follower
	// catch-up. Set to 0 to disable automatic snapshots.
	SnapshotThreshold uint64

	// SnapshotChunkSize controls the size of chunks when streaming snapshots
	// to followers. Larger chunks reduce RPC overhead but increase memory usage.
	SnapshotChunkSize int

	// LeaseEnabled enables lease-based reads for better read performance.
	// When enabled, the leader can serve reads without quorum confirmation
	// as long as the lease is valid. Requires clock synchronization assumptions.
	LeaseEnabled bool

	// LeaseDuration is the duration of the leader lease.
	// Must be less than ElectionTimeout - ClockDriftBound to ensure safety.
	// If not set (zero), defaults to ElectionTimeout * 0.9.
	LeaseDuration time.Duration

	// ClockDriftBound is the maximum expected clock drift between nodes.
	// Used to calculate safe lease duration. A conservative default is used
	// if not set. Typical values are in the range of 1-10ms for well-synchronized
	// clocks (e.g., NTP).
	ClockDriftBound time.Duration
}

// DefaultSnapshotChunkSize is the default chunk size for InstallSnapshot RPC (1MB).
const DefaultSnapshotChunkSize = 1024 * 1024

// LogStore provides persistent storage for the replicated log.
// Implementations must be crash-safe: entries written via StoreLogs must
// survive process restarts. This is critical for Raft correctness - losing
// acknowledged entries could cause committed data to be lost.
type LogStore interface {
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	GetLog(index uint64) (*api.LogEntry, error)
	StoreLogs(logs []*api.LogEntry) error
	DeleteRange(min, max uint64) error // Used for log compaction after snapshots
}

// StableStore provides persistent storage for Raft's critical state (term, votedFor).
// This state MUST be persisted before responding to any RPC to ensure correctness
// after crashes. Without this guarantee, a node could vote twice in the same term.
type StableStore interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, val []byte) error
	GetUint64(key []byte) (uint64, error)
	SetUint64(key []byte, val uint64) error
}

// StateMachine is the application-specific state that Raft replicates.
// Commands are applied in log order. The state machine MUST be deterministic:
// given the same sequence of commands, all nodes must arrive at identical state.
// Non-determinism (e.g., using wall-clock time) would cause replicas to diverge.
type StateMachine interface {
	Apply(logBytes []byte) interface{}
	Snapshot() (io.ReadCloser, error)
	Restore(rc io.ReadCloser) error
}

// CompactableLogStore extends LogStore with compaction tracking capabilities.
type CompactableLogStore interface {
	LogStore
	// SetCompactedIndex sets the compacted index for tracking compaction state.
	SetCompactedIndex(index uint64) error
}

// pendingSnapshotState tracks an in-progress snapshot installation from a leader.
// Snapshots arrive in chunks via InstallSnapshot RPC. We accumulate chunks in a
// temp file until the final chunk arrives, then atomically install the snapshot.
// This prevents partial snapshots from corrupting state if the transfer fails.
type pendingSnapshotState struct {
	meta       *SnapshotMeta
	file       *os.File
	bytesRecvd int64
}

// Raft implements the core consensus algorithm.
//
// Thread Safety: All public methods are safe for concurrent use. State modifications
// only occur within the main loop goroutine, which processes all events through
// a single select statement. Public getters acquire a read lock for safe access.
type Raft struct {
	// Persistent state (must be persisted before responding to RPCs)
	currentTerm uint64
	votedFor    string

	// Volatile state on all servers
	state       NodeState
	commitIndex uint64 // Highest log entry known to be committed
	lastApplied uint64 // Highest log entry applied to state machine

	// Volatile state on leaders (reinitialized after each election)
	// These track replication progress to each follower. Reinitialized on
	// becoming leader because we don't know followers' log state.
	nextIndex  map[string]uint64 // Next log index to send to each peer
	matchIndex map[string]uint64 // Highest log index known to be replicated on each peer

	leaderID      string         // Current known leader (empty if unknown)
	clusterConfig *ClusterConfig // Current cluster membership

	// Snapshot state - tracks the most recent snapshot for log compaction
	// and follower catch-up when they're too far behind
	snapshotStore     SnapshotStore
	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64
	snapshotting      bool // Guards against concurrent automatic snapshots

	pendingSnapshot *pendingSnapshotState // Follower-side snapshot installation

	// Lease state for lease-based reads
	leaseState *LeaseState

	// Dependencies
	logStore     LogStore
	stableStore  StableStore
	stateMachine StateMachine
	transport    transport.Transport
	config       Config

	// Event channels and timers
	rpcChan         <-chan transport.RPC
	stopChan        chan struct{}
	doneChan        chan struct{}
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	// Election tracking
	votesReceived map[string]bool
	electionTerm  uint64

	// Synchronization
	mu      sync.RWMutex
	running bool
}

// NewRaft creates a new Raft node. It loads persisted state from StableStore,
// restores the state machine from any existing snapshot, and initializes the
// node as a Follower. The node won't participate in consensus until Start() is called.
func NewRaft(config Config, logStore LogStore, stableStore StableStore,
	stateMachine StateMachine, trans transport.Transport, snapshotStore SnapshotStore) (*Raft, error) {

	// Load persisted state - these values survive restarts
	currentTerm, err := stableStore.GetUint64(keyCurrentTerm)
	if err != nil {
		return nil, err
	}

	votedForBytes, err := stableStore.Get(keyVotedFor)
	if err != nil {
		return nil, err
	}
	votedFor := string(votedForBytes)

	// Bootstrap cluster config from peers list. In production, this would
	// typically come from a configuration service or be persisted.
	clusterConfig := &ClusterConfig{
		Members: make([]ClusterMember, 0, len(config.Peers)+1),
	}
	clusterConfig.Members = append(clusterConfig.Members, ClusterMember{
		ID:      config.ID,
		Address: "",
		State:   Voter,
	})
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
			lastSnapshotIndex = meta.LastIncludedIndex
			lastSnapshotTerm = meta.LastIncludedTerm

			// Open snapshot to restore state machine
			_, reader, err := snapshotStore.Open()
			if err != nil {
				return nil, fmt.Errorf("failed to open snapshot for restore: %w", err)
			}

			if err := stateMachine.Restore(reader); err != nil {
				return nil, fmt.Errorf("failed to restore state machine from snapshot: %w", err)
			}

			commitIndex = meta.LastIncludedIndex
			lastApplied = meta.LastIncludedIndex

			if meta.Configuration != nil {
				clusterConfig = meta.Configuration
			}
		} else if err != ErrNoSnapshot {
			return nil, fmt.Errorf("failed to read snapshot metadata: %w", err)
		}
	}

	// Validate and set lease configuration if lease-based reads are enabled
	if config.LeaseEnabled {
		// Set default lease duration if not specified (90% of election timeout)
		if config.LeaseDuration == 0 {
			config.LeaseDuration = time.Duration(float64(config.ElectionTimeout) * 0.9)
		}

		// Validate lease duration safety: lease_duration < election_timeout - clock_drift_bound
		// This ensures the lease expires before a new leader can be elected
		safeDuration := config.ElectionTimeout - config.ClockDriftBound
		if config.LeaseDuration >= safeDuration {
			return nil, fmt.Errorf("lease duration (%v) must be less than election timeout (%v) minus clock drift bound (%v) = %v",
				config.LeaseDuration, config.ElectionTimeout, config.ClockDriftBound, safeDuration)
		}
	}

	r := &Raft{
		currentTerm: currentTerm,
		votedFor:    votedFor,

		state:       Follower,
		commitIndex: commitIndex,
		lastApplied: lastApplied,

		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),

		clusterConfig: clusterConfig,

		snapshotStore:     snapshotStore,
		lastSnapshotIndex: lastSnapshotIndex,
		lastSnapshotTerm:  lastSnapshotTerm,

		logStore:     logStore,
		stableStore:  stableStore,
		stateMachine: stateMachine,
		transport:    trans,

		config: config,

		rpcChan:  trans.Consumer(),
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),

		votesReceived: make(map[string]bool),
		electionTerm:  0,

		running: false,
	}

	// Initialize lease state if lease-based reads are enabled
	if config.LeaseEnabled {
		r.leaseState = NewLeaseState(config.LeaseDuration)
	}

	// Reconstruct cluster membership from committed LOG_CONFIG entries
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
func (r *Raft) VotesReceived() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.votesReceived)
}

// LastSnapshotIndex returns the lastIncludedIndex of the most recent snapshot.
func (r *Raft) LastSnapshotIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastSnapshotIndex
}

// LastSnapshotTerm returns the lastIncludedTerm of the most recent snapshot.
func (r *Raft) LastSnapshotTerm() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastSnapshotTerm
}

// GetConfiguration returns the current cluster configuration.
func (r *Raft) GetConfiguration() ClusterConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.clusterConfig == nil {
		return ClusterConfig{}
	}
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

// ApplyEntries is a public method to trigger entry application.
func (r *Raft) ApplyEntries() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applyEntries()
}

// SetCommitIndex sets the commit index directly for testing purposes.
func (r *Raft) SetCommitIndex(index uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commitIndex = index
}

// calculateQuorum returns the minimum number of nodes needed for a majority.
func calculateQuorum(voters []string) int {
	return (len(voters) / 2) + 1
}

// getVotersUnlocked returns a list of all voter node IDs without acquiring the lock.
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

// Start begins the Raft consensus loop.
func (r *Raft) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return nil
	}

	// Replay committed log entries to rebuild state machine
	if err := r.replayLogEntries(); err != nil {
		return fmt.Errorf("failed to replay log entries: %w", err)
	}

	r.electionTimer = time.NewTimer(r.randomElectionTimeout())
	r.heartbeatTicker = time.NewTicker(r.config.HeartbeatTimeout)

	r.running = true

	go r.run()

	return nil
}

// Stop gracefully shuts down the Raft node.
func (r *Raft) Stop() error {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return nil
	}
	r.running = false
	r.mu.Unlock()

	close(r.stopChan)
	<-r.doneChan

	return nil
}

// run is the main loop that handles all state transitions, RPC processing,
// and timer events through a select statement.
func (r *Raft) run() {
	defer close(r.doneChan)

	for {
		select {
		case <-r.stopChan:
			r.electionTimer.Stop()
			r.heartbeatTicker.Stop()
			return

		case <-r.electionTimer.C:
			r.mu.Lock()
			if r.state != Leader {
				_ = r.startElection()
			}
			r.electionTimer.Reset(r.randomElectionTimeout())
			r.mu.Unlock()

		case <-r.heartbeatTicker.C:
			r.mu.RLock()
			isLeader := r.state == Leader
			peers := r.config.Peers
			r.mu.RUnlock()

			if isLeader {
				for _, peer := range peers {
					r.sendAppendEntries(peer)
				}
			}

		case rpc := <-r.rpcChan:
			r.handleRPC(rpc)
		}
	}
}

// handleRPC processes an incoming RPC request and sends the response.
func (r *Raft) handleRPC(rpc transport.RPC) {
	var resp transport.RPCResponse

	switch req := rpc.Request.(type) {
	case *api.VoteRequest:
		r.mu.Lock()
		if req.Term > r.currentTerm {
			if err := r.stepDownToFollower(req.Term); err != nil {
				r.mu.Unlock()
				resp.Error = err
				rpc.RespChan <- resp
				return
			}
		}
		r.mu.Unlock()

		voteResp := r.handleVoteRequest(req)
		resp.Response = voteResp

	case *api.AppendEntriesRequest:
		r.mu.Lock()
		if req.Term > r.currentTerm {
			if err := r.stepDownToFollower(req.Term); err != nil {
				r.mu.Unlock()
				resp.Error = err
				rpc.RespChan <- resp
				return
			}
		} else if req.Term >= r.currentTerm && r.state == Candidate {
			r.state = Follower
			r.leaderID = string(req.LeaderId)
		}
		r.mu.Unlock()

		appendResp := r.handleAppendEntries(req)
		resp.Response = appendResp

	case *api.InstallSnapshotRequest:
		installResp := r.handleInstallSnapshot(req)
		resp.Response = installResp

	case *api.JoinClusterRequest:
		joinResp := r.handleJoinCluster(req)
		resp.Response = joinResp

	case *api.RemoveServerRequest:
		removeResp := r.handleRemoveServer(req)
		resp.Response = removeResp

	default:
		resp.Error = errors.New("unknown RPC type")
	}

	rpc.RespChan <- resp
}

// JoinCluster handles a request from a new node to join the cluster.
func (r *Raft) JoinCluster(nodeID, addr string) error {
	r.mu.Lock()

	if r.state != Leader {
		leaderHint := r.leaderID
		r.mu.Unlock()
		return &NotLeaderError{LeaderHint: leaderHint}
	}

	if !r.running {
		r.mu.Unlock()
		return ErrStopped
	}

	// Check if nodeID already exists (idempotency)
	for _, member := range r.clusterConfig.Members {
		if member.ID == nodeID {
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

	configData, err := SerializeConfig(newConfig)
	if err != nil {
		r.mu.Unlock()
		return err
	}

	lastIndex, err := r.logStore.LastIndex()
	if err != nil {
		r.mu.Unlock()
		return err
	}
	newIndex := lastIndex + 1

	entry := &api.LogEntry{
		Index: newIndex,
		Term:  r.currentTerm,
		Type:  api.LogType_LOG_CONFIGURATION,
		Data:  configData,
	}

	if err := r.logStore.StoreLogs([]*api.LogEntry{entry}); err != nil {
		r.mu.Unlock()
		return err
	}

	r.nextIndex[addr] = newIndex + 1
	r.matchIndex[addr] = 0

	r.mu.Unlock()

	// Wait for commit
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
type NotLeaderError struct {
	LeaderHint string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderHint != "" {
		return "node is not the leader; leader is " + e.LeaderHint
	}
	return "node is not the leader"
}

func (e *NotLeaderError) Is(target error) bool {
	return target == ErrNotLeader
}

// handleJoinCluster processes an incoming JoinCluster RPC.
func (r *Raft) handleJoinCluster(req *api.JoinClusterRequest) *api.JoinClusterResponse {
	err := r.JoinCluster(req.NodeId, req.Address)
	if err != nil {
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
func (r *Raft) RemoveServer(nodeID string) error {
	r.mu.Lock()

	if r.state != Leader {
		leaderHint := r.leaderID
		r.mu.Unlock()
		return &NotLeaderError{LeaderHint: leaderHint}
	}

	if !r.running {
		r.mu.Unlock()
		return ErrStopped
	}

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

	configData, err := SerializeConfig(newConfig)
	if err != nil {
		r.mu.Unlock()
		return err
	}

	lastIndex, err := r.logStore.LastIndex()
	if err != nil {
		r.mu.Unlock()
		return err
	}
	newIndex := lastIndex + 1

	entry := &api.LogEntry{
		Index: newIndex,
		Term:  r.currentTerm,
		Type:  api.LogType_LOG_CONFIGURATION,
		Data:  configData,
	}

	if err := r.logStore.StoreLogs([]*api.LogEntry{entry}); err != nil {
		r.mu.Unlock()
		return err
	}

	r.mu.Unlock()

	// Wait for commit
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

// handleRemoveServer processes an incoming RemoveServer RPC.
func (r *Raft) handleRemoveServer(req *api.RemoveServerRequest) *api.RemoveServerResponse {
	err := r.RemoveServer(req.NodeId)
	if err != nil {
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

// compactLog deletes log entries from FirstIndex to lastSnapshotIndex.
func (r *Raft) compactLog() error {
	if r.lastSnapshotIndex == 0 {
		return nil
	}

	firstIndex, err := r.logStore.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get first log index: %w", err)
	}

	if firstIndex == 0 {
		if compactable, ok := r.logStore.(CompactableLogStore); ok {
			if err := compactable.SetCompactedIndex(r.lastSnapshotIndex); err != nil {
				return fmt.Errorf("failed to set compacted index: %w", err)
			}
		}
		return nil
	}

	if firstIndex > r.lastSnapshotIndex {
		return nil
	}

	if err := r.logStore.DeleteRange(firstIndex, r.lastSnapshotIndex); err != nil {
		return fmt.Errorf("failed to delete log range [%d, %d]: %w", firstIndex, r.lastSnapshotIndex, err)
	}

	if compactable, ok := r.logStore.(CompactableLogStore); ok {
		if err := compactable.SetCompactedIndex(r.lastSnapshotIndex); err != nil {
			return fmt.Errorf("failed to set compacted index: %w", err)
		}
	}

	return nil
}

// CompactLog is a public method to trigger log compaction after a snapshot.
func (r *Raft) CompactLog() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.compactLog()
}
