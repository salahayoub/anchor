// Package raft provides read-only query types and functionality for the Raft consensus algorithm.
//
// read_only.go contains:
// - ConsistencyLevel type and constants for read consistency guarantees
// - ReadRequest and ReadResponse types for read operations
// - Read-related error types
// - LeaseState for lease-based read operations
// - ReadMetrics for tracking read operation statistics
package raft

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ReadMetrics tracks statistics for read operations.
// All counters are safe for concurrent access using atomic operations.
//
// Thread Safety: ReadMetrics is safe for concurrent use. All counter fields
// use atomic operations for thread-safe increments and reads.
type ReadMetrics struct {
	// Counters for reads by consistency level
	LinearizableReads uint64 // Total linearizable reads performed
	LeaseReads        uint64 // Total lease-based reads performed
	StaleReads        uint64 // Total stale reads performed

	// Latency tracking (in nanoseconds for precision)
	LinearizableLatencyTotal uint64 // Total latency for linearizable reads
	LinearizableLatencyCount uint64 // Count for calculating average
	LeaseLatencyTotal        uint64 // Total latency for lease reads
	LeaseLatencyCount        uint64 // Count for calculating average
	StaleLatencyTotal        uint64 // Total latency for stale reads
	StaleLatencyCount        uint64 // Count for calculating average

	// Lease event counters
	LeaseExtensions  uint64 // Number of times the lease was extended/renewed
	LeaseExpirations uint64 // Number of times the lease expired

	// Quorum confirmation counter
	QuorumConfirmations uint64 // Number of successful quorum confirmations
	QuorumTimeouts      uint64 // Number of quorum confirmation timeouts
}

// NewReadMetrics creates a new ReadMetrics instance with all counters initialized to zero.
func NewReadMetrics() *ReadMetrics {
	return &ReadMetrics{}
}

// IncrementLinearizableReads atomically increments the linearizable read counter.
func (m *ReadMetrics) IncrementLinearizableReads() {
	atomic.AddUint64(&m.LinearizableReads, 1)
}

// IncrementLeaseReads atomically increments the lease read counter.
func (m *ReadMetrics) IncrementLeaseReads() {
	atomic.AddUint64(&m.LeaseReads, 1)
}

// IncrementStaleReads atomically increments the stale read counter.
func (m *ReadMetrics) IncrementStaleReads() {
	atomic.AddUint64(&m.StaleReads, 1)
}

// RecordLinearizableLatency records the latency for a linearizable read operation.
func (m *ReadMetrics) RecordLinearizableLatency(latency time.Duration) {
	atomic.AddUint64(&m.LinearizableLatencyTotal, uint64(latency.Nanoseconds()))
	atomic.AddUint64(&m.LinearizableLatencyCount, 1)
}

// RecordLeaseLatency records the latency for a lease read operation.
func (m *ReadMetrics) RecordLeaseLatency(latency time.Duration) {
	atomic.AddUint64(&m.LeaseLatencyTotal, uint64(latency.Nanoseconds()))
	atomic.AddUint64(&m.LeaseLatencyCount, 1)
}

// RecordStaleLatency records the latency for a stale read operation.
func (m *ReadMetrics) RecordStaleLatency(latency time.Duration) {
	atomic.AddUint64(&m.StaleLatencyTotal, uint64(latency.Nanoseconds()))
	atomic.AddUint64(&m.StaleLatencyCount, 1)
}

// IncrementLeaseExtensions atomically increments the lease extension counter.
func (m *ReadMetrics) IncrementLeaseExtensions() {
	atomic.AddUint64(&m.LeaseExtensions, 1)
}

// IncrementLeaseExpirations atomically increments the lease expiration counter.
func (m *ReadMetrics) IncrementLeaseExpirations() {
	atomic.AddUint64(&m.LeaseExpirations, 1)
}

// IncrementQuorumConfirmations atomically increments the quorum confirmation counter.
func (m *ReadMetrics) IncrementQuorumConfirmations() {
	atomic.AddUint64(&m.QuorumConfirmations, 1)
}

// IncrementQuorumTimeouts atomically increments the quorum timeout counter.
func (m *ReadMetrics) IncrementQuorumTimeouts() {
	atomic.AddUint64(&m.QuorumTimeouts, 1)
}

// GetLinearizableReads returns the current linearizable read count.
func (m *ReadMetrics) GetLinearizableReads() uint64 {
	return atomic.LoadUint64(&m.LinearizableReads)
}

// GetLeaseReads returns the current lease read count.
func (m *ReadMetrics) GetLeaseReads() uint64 {
	return atomic.LoadUint64(&m.LeaseReads)
}

// GetStaleReads returns the current stale read count.
func (m *ReadMetrics) GetStaleReads() uint64 {
	return atomic.LoadUint64(&m.StaleReads)
}

// GetTotalReads returns the total number of reads across all consistency levels.
func (m *ReadMetrics) GetTotalReads() uint64 {
	return m.GetLinearizableReads() + m.GetLeaseReads() + m.GetStaleReads()
}

// GetAverageLinearizableLatency returns the average latency for linearizable reads.
// Returns 0 if no linearizable reads have been recorded.
func (m *ReadMetrics) GetAverageLinearizableLatency() time.Duration {
	count := atomic.LoadUint64(&m.LinearizableLatencyCount)
	if count == 0 {
		return 0
	}
	total := atomic.LoadUint64(&m.LinearizableLatencyTotal)
	return time.Duration(total / count)
}

// GetAverageLeaseLatency returns the average latency for lease reads.
// Returns 0 if no lease reads have been recorded.
func (m *ReadMetrics) GetAverageLeaseLatency() time.Duration {
	count := atomic.LoadUint64(&m.LeaseLatencyCount)
	if count == 0 {
		return 0
	}
	total := atomic.LoadUint64(&m.LeaseLatencyTotal)
	return time.Duration(total / count)
}

// GetAverageStaleLatency returns the average latency for stale reads.
// Returns 0 if no stale reads have been recorded.
func (m *ReadMetrics) GetAverageStaleLatency() time.Duration {
	count := atomic.LoadUint64(&m.StaleLatencyCount)
	if count == 0 {
		return 0
	}
	total := atomic.LoadUint64(&m.StaleLatencyTotal)
	return time.Duration(total / count)
}

// GetLeaseExtensions returns the current lease extension count.
func (m *ReadMetrics) GetLeaseExtensions() uint64 {
	return atomic.LoadUint64(&m.LeaseExtensions)
}

// GetLeaseExpirations returns the current lease expiration count.
func (m *ReadMetrics) GetLeaseExpirations() uint64 {
	return atomic.LoadUint64(&m.LeaseExpirations)
}

// GetQuorumConfirmations returns the current quorum confirmation count.
func (m *ReadMetrics) GetQuorumConfirmations() uint64 {
	return atomic.LoadUint64(&m.QuorumConfirmations)
}

// GetQuorumTimeouts returns the current quorum timeout count.
func (m *ReadMetrics) GetQuorumTimeouts() uint64 {
	return atomic.LoadUint64(&m.QuorumTimeouts)
}

// Copy returns a snapshot copy of the current metrics.
// This is useful for getting a consistent view of all metrics at a point in time.
func (m *ReadMetrics) Copy() *ReadMetrics {
	return &ReadMetrics{
		LinearizableReads:        atomic.LoadUint64(&m.LinearizableReads),
		LeaseReads:               atomic.LoadUint64(&m.LeaseReads),
		StaleReads:               atomic.LoadUint64(&m.StaleReads),
		LinearizableLatencyTotal: atomic.LoadUint64(&m.LinearizableLatencyTotal),
		LinearizableLatencyCount: atomic.LoadUint64(&m.LinearizableLatencyCount),
		LeaseLatencyTotal:        atomic.LoadUint64(&m.LeaseLatencyTotal),
		LeaseLatencyCount:        atomic.LoadUint64(&m.LeaseLatencyCount),
		StaleLatencyTotal:        atomic.LoadUint64(&m.StaleLatencyTotal),
		StaleLatencyCount:        atomic.LoadUint64(&m.StaleLatencyCount),
		LeaseExtensions:          atomic.LoadUint64(&m.LeaseExtensions),
		LeaseExpirations:         atomic.LoadUint64(&m.LeaseExpirations),
		QuorumConfirmations:      atomic.LoadUint64(&m.QuorumConfirmations),
		QuorumTimeouts:           atomic.LoadUint64(&m.QuorumTimeouts),
	}
}

// ConsistencyLevel represents the consistency guarantee for read operations.
// Different levels trade off between consistency and performance.
type ConsistencyLevel int

const (
	// Linearizable provides the strongest consistency guarantee.
	// Reads are guaranteed to see the most recent committed write.
	// Requires leader confirmation via quorum heartbeat.
	Linearizable ConsistencyLevel = iota

	// LeaseRead provides strong consistency with better performance.
	// Uses leader lease to avoid quorum confirmation on every read.
	// Requires clock synchronization assumptions.
	LeaseRead

	// Stale allows reading from any node without consistency guarantees.
	// Fastest option but may return stale data.
	// Useful for read-heavy workloads where eventual consistency is acceptable.
	Stale
)

// String returns a human-readable representation of the ConsistencyLevel.
func (c ConsistencyLevel) String() string {
	switch c {
	case Linearizable:
		return "linearizable"
	case LeaseRead:
		return "lease"
	case Stale:
		return "stale"
	default:
		return "unknown"
	}
}

// ErrInvalidConsistency is returned when an invalid consistency level is specified.
var ErrInvalidConsistency = errors.New("invalid consistency level")

// ParseConsistencyLevel parses a string into a ConsistencyLevel.
// Valid values are: "linearizable", "lease", "stale" (case-insensitive).
// Returns ErrInvalidConsistency for invalid values.
func ParseConsistencyLevel(s string) (ConsistencyLevel, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "linearizable":
		return Linearizable, nil
	case "lease":
		return LeaseRead, nil
	case "stale":
		return Stale, nil
	default:
		return 0, ErrInvalidConsistency
	}
}

// ReadRequest represents a request to read a value from the state machine.
type ReadRequest struct {
	// Key is the key to read from the state machine.
	Key string

	// Consistency specifies the consistency level for this read.
	Consistency ConsistencyLevel

	// RequestID is a unique identifier for this request, used for tracking and debugging.
	RequestID string
}

// ReadResponse represents the response to a read request.
type ReadResponse struct {
	// Value is the value read from the state machine.
	// Empty if Found is false.
	Value []byte

	// Found indicates whether the key was found in the state machine.
	Found bool

	// AppliedIndex is the log index at which the read was performed.
	// Useful for debugging and consistency verification.
	AppliedIndex uint64

	// Term is the term at which the read was performed.
	Term uint64

	// Consistency is the consistency level that was used for this read.
	Consistency ConsistencyLevel

	// Error contains any error that occurred during the read.
	// nil if the read was successful.
	Error error
}

// Sentinel errors for read operations. Using sentinel errors enables callers
// to use errors.Is() for reliable error handling even when errors are wrapped.
var (
	// ErrQuorumTimeout is returned when a quorum confirmation times out.
	// This can happen during linearizable reads when the leader cannot
	// confirm its leadership with a majority of nodes.
	ErrQuorumTimeout = errors.New("quorum confirmation timed out")

	// ErrLeaseExpired is returned when the leader lease has expired.
	// This can happen during lease-based reads when the lease duration
	// has elapsed since the last successful heartbeat.
	ErrLeaseExpired = errors.New("leader lease has expired")

	// ErrReadTimeout is returned when a read operation times out.
	// This is a general timeout error for read operations.
	ErrReadTimeout = errors.New("read operation timed out")
)

// LeaseState tracks the leader lease for lease-based reads.
// The lease allows the leader to serve reads without quorum confirmation
// as long as the lease is valid. The lease is renewed on successful heartbeats.
//
// Thread Safety: LeaseState is safe for concurrent use. All methods acquire
// the mutex for safe access to the lease state.
type LeaseState struct {
	mu       sync.RWMutex
	start    time.Time     // When the lease was last renewed (monotonic)
	duration time.Duration // How long the lease is valid
	valid    bool          // Whether the lease has been initialized
}

// NewLeaseState creates a new LeaseState with the specified duration.
// The lease starts in an invalid state and must be renewed before use.
func NewLeaseState(duration time.Duration) *LeaseState {
	return &LeaseState{
		duration: duration,
		valid:    false,
	}
}

// IsValid returns true if the lease is currently valid.
// A lease is valid if it has been renewed and has not expired.
// Uses monotonic time to avoid issues with wall clock adjustments.
func (l *LeaseState) IsValid() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if !l.valid {
		return false
	}

	// Use time.Since which uses monotonic clock for elapsed time calculation
	elapsed := time.Since(l.start)
	return elapsed < l.duration
}

// Renew updates the lease start time to now, making the lease valid.
// This should be called when the leader successfully confirms its leadership
// (e.g., after receiving successful heartbeat responses from a quorum).
func (l *LeaseState) Renew() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.start = time.Now() // time.Now() includes monotonic clock reading
	l.valid = true
}

// Invalidate marks the lease as invalid.
// This should be called when the node steps down from leadership.
func (l *LeaseState) Invalidate() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.valid = false
}

// Remaining returns the remaining duration of the lease.
// Returns 0 if the lease is invalid or has expired.
func (l *LeaseState) Remaining() time.Duration {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if !l.valid {
		return 0
	}

	elapsed := time.Since(l.start)
	remaining := l.duration - elapsed
	if remaining < 0 {
		return 0
	}
	return remaining
}

// Duration returns the configured lease duration.
func (l *LeaseState) Duration() time.Duration {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.duration
}

// CalculateSafeLeaseDuration calculates a safe lease duration given the
// election timeout and clock drift bound. The lease duration must be less
// than (election_timeout - clock_drift_bound) to ensure safety.
//
// This ensures that even with maximum clock drift, the lease will expire
// before a new leader can be elected, preventing split-brain scenarios.
func CalculateSafeLeaseDuration(electionTimeout, clockDriftBound time.Duration) time.Duration {
	safeDuration := electionTimeout - clockDriftBound
	if safeDuration <= 0 {
		return 0
	}
	return safeDuration
}

// ValidateLeaseDuration checks if the given lease duration is safe given
// the election timeout and clock drift bound. Returns true if safe.
//
// Safety requirement: lease_duration < election_timeout - clock_drift_bound
func ValidateLeaseDuration(leaseDuration, electionTimeout, clockDriftBound time.Duration) bool {
	safeDuration := CalculateSafeLeaseDuration(electionTimeout, clockDriftBound)
	return leaseDuration > 0 && leaseDuration < safeDuration
}

// readIndexState tracks the state of a pending read-index request.
// Each linearizable read requires confirmation from a quorum of nodes
// to ensure the leader is still the valid leader.
type readIndexState struct {
	// requestID is a unique identifier for this read request
	requestID string

	// readIndex is the commit index at the time the read was initiated
	readIndex uint64

	// acks tracks which peers have acknowledged this read request
	// Key is the peer ID, value is true if acknowledged
	acks map[string]bool

	// respChan is used to signal when the read can proceed or has failed
	respChan chan error

	// createdAt is when this read request was created (for timeout tracking)
	createdAt time.Time
}

// newReadIndexState creates a new readIndexState with the given parameters.
func newReadIndexState(requestID string, readIndex uint64) *readIndexState {
	return &readIndexState{
		requestID: requestID,
		readIndex: readIndex,
		acks:      make(map[string]bool),
		respChan:  make(chan error, 1),
		createdAt: time.Now(),
	}
}

// AddAck records an acknowledgment from a peer for this read request.
func (r *readIndexState) AddAck(peerID string) {
	r.acks[peerID] = true
}

// HasQuorum returns true if a majority of voters have acknowledged this read.
// The voters slice should include all voting members of the cluster.
func (r *readIndexState) HasQuorum(voters []string) bool {
	if len(voters) == 0 {
		return false
	}

	quorum := (len(voters) / 2) + 1
	ackCount := 0

	for _, voter := range voters {
		if r.acks[voter] {
			ackCount++
		}
	}

	return ackCount >= quorum
}

// ReadOnly manages pending read-index requests for linearizable reads.
// It tracks pending reads and handles quorum confirmation and timeouts.
//
// Thread Safety: ReadOnly is safe for concurrent use. All methods acquire
// the mutex for safe access to the pending reads map.
type ReadOnly struct {
	mu sync.RWMutex

	// pendingReads maps request IDs to their read index state
	pendingReads map[string]*readIndexState

	// lease tracks the leader lease for lease-based reads
	lease *LeaseState

	// readTimeout is the maximum time to wait for quorum confirmation
	readTimeout time.Duration

	// stopChan signals the cleanup goroutine to stop
	stopChan chan struct{}

	// doneChan signals that the cleanup goroutine has stopped
	doneChan chan struct{}
}

// NewReadOnly creates a new ReadOnly instance with the specified timeout.
// If lease is provided, it will be used for lease-based reads.
func NewReadOnly(readTimeout time.Duration, lease *LeaseState) *ReadOnly {
	ro := &ReadOnly{
		pendingReads: make(map[string]*readIndexState),
		lease:        lease,
		readTimeout:  readTimeout,
		stopChan:     make(chan struct{}),
		doneChan:     make(chan struct{}),
	}

	// Start background cleanup goroutine
	go ro.cleanupLoop()

	return ro
}

// AddPendingRead creates a new pending read request and returns the response channel.
// The caller should wait on the returned channel for the read to be confirmed or timeout.
func (ro *ReadOnly) AddPendingRead(requestID string, readIndex uint64) <-chan error {
	ro.mu.Lock()
	defer ro.mu.Unlock()

	state := newReadIndexState(requestID, readIndex)
	ro.pendingReads[requestID] = state

	return state.respChan
}

// AckRead records an acknowledgment from a peer for a pending read request.
// If the read achieves quorum after this ack, it signals success on the response channel.
func (ro *ReadOnly) AckRead(requestID string, peerID string, voters []string) {
	ro.mu.Lock()
	defer ro.mu.Unlock()

	state, exists := ro.pendingReads[requestID]
	if !exists {
		return
	}

	state.AddAck(peerID)

	// Check if we now have quorum
	if state.HasQuorum(voters) {
		// Signal success and remove from pending
		select {
		case state.respChan <- nil:
		default:
			// Channel already has a value, skip
		}
		delete(ro.pendingReads, requestID)
	}
}

// GetPendingRead returns the read index state for a pending read request.
// Returns nil if the request doesn't exist.
func (ro *ReadOnly) GetPendingRead(requestID string) *readIndexState {
	ro.mu.RLock()
	defer ro.mu.RUnlock()
	return ro.pendingReads[requestID]
}

// RemovePendingRead removes a pending read request.
func (ro *ReadOnly) RemovePendingRead(requestID string) {
	ro.mu.Lock()
	defer ro.mu.Unlock()
	delete(ro.pendingReads, requestID)
}

// PendingReadCount returns the number of pending read requests.
func (ro *ReadOnly) PendingReadCount() int {
	ro.mu.RLock()
	defer ro.mu.RUnlock()
	return len(ro.pendingReads)
}

// GetPendingReadIDs returns a slice of all pending read request IDs.
// This is used to include read IDs in AppendEntries requests for confirmation.
func (ro *ReadOnly) GetPendingReadIDs() []string {
	ro.mu.RLock()
	defer ro.mu.RUnlock()

	if len(ro.pendingReads) == 0 {
		return nil
	}

	ids := make([]string, 0, len(ro.pendingReads))
	for id := range ro.pendingReads {
		ids = append(ids, id)
	}
	return ids
}

// cleanupLoop runs in the background and cleans up timed-out read requests.
func (ro *ReadOnly) cleanupLoop() {
	defer close(ro.doneChan)

	ticker := time.NewTicker(ro.readTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ro.stopChan:
			return
		case <-ticker.C:
			ro.cleanupTimedOutReads()
		}
	}
}

// cleanupTimedOutReads removes read requests that have exceeded the timeout
// and sends ErrQuorumTimeout to their response channels.
func (ro *ReadOnly) cleanupTimedOutReads() {
	ro.mu.Lock()
	defer ro.mu.Unlock()

	now := time.Now()
	for requestID, state := range ro.pendingReads {
		if now.Sub(state.createdAt) > ro.readTimeout {
			// Send timeout error
			select {
			case state.respChan <- ErrQuorumTimeout:
			default:
				// Channel already has a value, skip
			}
			delete(ro.pendingReads, requestID)
		}
	}
}

// Stop stops the background cleanup goroutine.
func (ro *ReadOnly) Stop() {
	close(ro.stopChan)
	<-ro.doneChan
}

// GetLease returns the lease state for lease-based reads.
func (ro *ReadOnly) GetLease() *LeaseState {
	return ro.lease
}

// ReadableStateMachine extends StateMachine with read capabilities.
// This interface allows reading values from the state machine without
// going through the Apply method.
type ReadableStateMachine interface {
	StateMachine
	// Get retrieves a value by key from the state machine.
	// Returns the value and true if found, empty and false otherwise.
	Get(key string) (string, bool)
}

// StaleReadResult contains the result of a stale read operation.
type StaleReadResult struct {
	// Value is the value read from the state machine.
	Value string
	// Found indicates whether the key was found.
	Found bool
	// AppliedIndex is the log index at which the read was performed.
	AppliedIndex uint64
	// Term is the current term at the time of the read.
	Term uint64
}
