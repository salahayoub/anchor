// Package fsm provides the Finite State Machine implementation for the Raft consensus protocol.
//
// # Thread Safety Guarantees
//
// KVStore is safe for concurrent use by multiple goroutines. This safety is provided
// by a sync.RWMutex protecting all map operations:
//
//   - Read operations (Get, Snapshot) acquire a read lock allowing concurrent readers
//   - Write operations (Apply, Restore) acquire a write lock ensuring exclusive access
//
// This means:
//   - Multiple goroutines can safely call Get and Snapshot concurrently
//   - Write operations are serialized automatically
//   - Mixed read and write operations from different goroutines are safe
//
// References:
package fsm

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sync"
)

// Error types for FSM operations
var (
	// ErrInvalidJSON indicates that the provided data is not valid JSON
	ErrInvalidJSON = errors.New("invalid JSON format")
	// ErrNilReader indicates that a nil reader was provided to Restore
	ErrNilReader = errors.New("nil reader provided")
)

// Command represents a JSON-encoded log command for the KVStore.
// Commands are applied to the state machine to modify its state.
type Command struct {
	Op    string `json:"op"`              // Operation type (e.g., "set")
	Key   string `json:"key"`             // Key to operate on
	Value string `json:"value,omitempty"` // Value for set operations
}

// StateMachine defines the interface for Raft-managed application state.
// Implementations must be thread-safe for concurrent access.
type StateMachine interface {
	// Apply takes command bytes and modifies state, returning any result.
	// Returns an error for malformed input, nil for successful operations
	// or unknown operations that should be ignored.
	Apply(logBytes []byte) interface{}

	// Snapshot returns a reader containing serialized state.
	// The caller is responsible for closing the returned reader.
	Snapshot() (io.ReadCloser, error)

	// Restore replaces current state with data from the reader.
	// On success, the reader is closed. On error, existing state is preserved.
	Restore(rc io.ReadCloser) error
}

// KVStore implements StateMachine as a thread-safe in-memory key-value store.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVStore creates a new empty KVStore.
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Apply parses and executes a JSON command, modifying the store state.
// Returns nil on success or for unknown operations, error for malformed JSON.
func (kv *KVStore) Apply(logBytes []byte) interface{} {
	var cmd Command
	if err := json.Unmarshal(logBytes, &cmd); err != nil {
		return ErrInvalidJSON
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch cmd.Op {
	case "set":
		kv.data[cmd.Key] = cmd.Value
	}
	// Unknown operations are ignored and return nil

	return nil
}

// Get retrieves a value by key from the store.
// Returns the value and true if the key exists, empty string and false otherwise.
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	value, ok := kv.data[key]
	return value, ok
}

// Snapshot returns a reader containing JSON-encoded state.
// Creates a point-in-time copy of the state while holding the read lock,
// then releases the lock before JSON encoding. This minimizes lock contention
// while ensuring a consistent snapshot.
func (kv *KVStore) Snapshot() (io.ReadCloser, error) {
	kv.mu.RLock()
	clone := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		clone[k] = v
	}
	kv.mu.RUnlock()

	// JSON-encode outside the lock to avoid blocking readers
	data, err := json.Marshal(clone)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

// Restore replaces current state with data from the reader.
// On error, existing state is preserved (atomic replacement).
// The reader is closed on success to signal completion to the caller.
func (kv *KVStore) Restore(rc io.ReadCloser) error {
	if rc == nil {
		return ErrNilReader
	}

	// Read all data from the reader
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	// Decode JSON into a new map
	var newData map[string]string
	if err := json.Unmarshal(data, &newData); err != nil {
		return ErrInvalidJSON
	}

	// Acquire write lock and replace the map
	kv.mu.Lock()
	kv.data = newData
	kv.mu.Unlock()

	// Close the reader on success
	return rc.Close()
}
