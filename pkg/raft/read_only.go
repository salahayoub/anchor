// Package raft provides read-only query types and functionality for the Raft consensus algorithm.
//
// read_only.go contains:
// - ConsistencyLevel type and constants for read consistency guarantees
// - ReadRequest and ReadResponse types for read operations
// - Read-related error types
// - LeaseState for lease-based read operations
package raft

import (
	"errors"
	"strings"
	"sync"
	"time"
)

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
