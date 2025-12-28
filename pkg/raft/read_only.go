// Package raft provides read-only query types and functionality for the Raft consensus algorithm.
//
// read_only.go contains:
// - ConsistencyLevel type and constants for read consistency guarantees
// - ReadRequest and ReadResponse types for read operations
// - Read-related error types
package raft

import (
	"errors"
	"strings"
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
