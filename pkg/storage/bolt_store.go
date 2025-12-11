// Package storage provides persistent storage implementations for the Raft consensus engine.
//
// # Thread Safety Guarantees
//
// BoltStore is safe for concurrent use by multiple goroutines. This safety is provided
// by BoltDB's transaction model:
//
//   - BoltDB allows multiple concurrent read transactions (View)
//   - BoltDB allows only one write transaction (Update) at a time
//   - Write transactions are serialized automatically by BoltDB's internal locking
//   - Read transactions see a consistent snapshot of the database
//   - Readers do not block writers, and writers do not block readers
//
// This means:
//   - Multiple goroutines can safely call FirstIndex, LastIndex, GetLog, Get, GetUint64 concurrently
//   - Multiple goroutines can safely call StoreLogs, DeleteRange, Set, SetUint64 concurrently
//     (BoltDB will serialize the writes internally)
//   - Mixed read and write operations from different goroutines are safe
//
// The BoltStore implementation does not add any additional locking beyond what BoltDB provides,
// as BoltDB's transaction isolation is sufficient for Raft's requirements.
//
// References:
//   - BoltDB documentation: https://github.com/etcd-io/bbolt#transactions
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/salahayoub/anchor/api"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

// Bucket names for BoltDB storage
var (
	logsBucket   = []byte("logs")
	stableBucket = []byte("stable")
)

// Key for storing compacted index in stable bucket
var keyCompactedIndex = []byte("compactedIndex")

// Error types
var (
	ErrLogNotFound = errors.New("log entry not found")
	ErrKeyNotFound = errors.New("key not found")
	// ErrCompacted is returned when accessing a log entry that has been compacted.
	ErrCompacted = errors.New("log entry has been compacted")
)

// BoltStore implements both LogStore and StableStore using BoltDB.
// It provides persistent storage for Raft log entries and stable state.
//
// BoltStore is safe for concurrent use by multiple goroutines.
// Thread safety is guaranteed by BoltDB's transaction isolation:
//   - Read operations (View) can run concurrently
//   - Write operations (Update) are serialized automatically
//   - No additional locking is required
type BoltStore struct {
	db             *bbolt.DB
	path           string
	compactedIndex uint64 // Index of the last compacted log entry (cached)
}

// NewBoltStore creates a new BoltStore at the specified path.
// It opens or creates the database file and initializes the required buckets.
func NewBoltStore(path string) (*BoltStore, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bolt database: %w", err)
	}

	// Create buckets if they don't exist
	err = db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(logsBucket); err != nil {
			return fmt.Errorf("failed to create logs bucket: %w", err)
		}
		if _, err := tx.CreateBucketIfNotExists(stableBucket); err != nil {
			return fmt.Errorf("failed to create stable bucket: %w", err)
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	store := &BoltStore{
		db:   db,
		path: path,
	}

	// Load compactedIndex from stable storage
	compactedIndex, err := store.GetUint64(keyCompactedIndex)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load compacted index: %w", err)
	}
	store.compactedIndex = compactedIndex

	return store, nil
}

// Close releases all database resources.
func (b *BoltStore) Close() error {
	return b.db.Close()
}

// uint64ToBytes encodes a uint64 value to big-endian bytes.
// Big-endian encoding ensures proper lexicographic ordering of keys.
func uint64ToBytes(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

// bytesToUint64 decodes big-endian bytes to a uint64 value.
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// ============================================================================
// LogStore Interface Implementation
// ============================================================================

// FirstIndex returns the first index written to the log store.
// Returns 0 if the log store is empty and no compaction has occurred.
// After compaction, returns max(firstLogEntry, compactedIndex + 1).
func (b *BoltStore) FirstIndex() (uint64, error) {
	var firstLogEntry uint64
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(logsBucket)
		cursor := bucket.Cursor()
		key, _ := cursor.First()
		if key == nil {
			firstLogEntry = 0
			return nil
		}
		firstLogEntry = bytesToUint64(key)
		return nil
	})
	if err != nil {
		return 0, err
	}

	// If log is empty but compaction has occurred, return compactedIndex + 1
	if firstLogEntry == 0 && b.compactedIndex > 0 {
		return b.compactedIndex + 1, nil
	}

	// Return max(firstLogEntry, compactedIndex + 1)
	if b.compactedIndex > 0 && b.compactedIndex+1 > firstLogEntry {
		return b.compactedIndex + 1, nil
	}

	return firstLogEntry, nil
}

// LastIndex returns the last index written to the log store.
// Returns 0 if the log store is empty and no compaction has occurred.
// After compaction, returns max(lastLogEntry, compactedIndex) when log is empty.
func (b *BoltStore) LastIndex() (uint64, error) {
	var lastLogEntry uint64
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(logsBucket)
		cursor := bucket.Cursor()
		key, _ := cursor.Last()
		if key == nil {
			lastLogEntry = 0
			return nil
		}
		lastLogEntry = bytesToUint64(key)
		return nil
	})
	if err != nil {
		return 0, err
	}

	// If log is empty but compaction has occurred, return compactedIndex
	if lastLogEntry == 0 && b.compactedIndex > 0 {
		return b.compactedIndex, nil
	}

	// Return max(lastLogEntry, compactedIndex)
	if b.compactedIndex > lastLogEntry {
		return b.compactedIndex, nil
	}

	return lastLogEntry, nil
}

// StoreLogs stores multiple log entries in a single batch transaction.
// It serializes each LogEntry using protobuf and stores with big-endian encoded index as key.
func (b *BoltStore) StoreLogs(logs []*api.LogEntry) error {
	if len(logs) == 0 {
		return nil
	}

	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(logsBucket)
		for _, log := range logs {
			key := uint64ToBytes(log.Index)
			val, err := proto.Marshal(log)
			if err != nil {
				return fmt.Errorf("failed to serialize log entry: %w", err)
			}
			if err := bucket.Put(key, val); err != nil {
				return fmt.Errorf("failed to store log entry: %w", err)
			}
		}
		return nil
	})
}

// GetLog retrieves a log entry at the specified index.
// Returns ErrLogNotFound if the entry does not exist or index is 0.
// Returns ErrCompacted if the entry has been compacted (index <= compactedIndex).
func (b *BoltStore) GetLog(index uint64) (*api.LogEntry, error) {
	if index == 0 {
		return nil, ErrLogNotFound
	}

	// Check if the entry has been compacted
	if b.compactedIndex > 0 && index <= b.compactedIndex {
		return nil, ErrCompacted
	}

	var entry *api.LogEntry
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(logsBucket)
		key := uint64ToBytes(index)
		val := bucket.Get(key)
		if val == nil {
			return ErrLogNotFound
		}

		entry = &api.LogEntry{}
		if err := proto.Unmarshal(val, entry); err != nil {
			return fmt.Errorf("failed to deserialize log entry: %w", err)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return entry, nil
}

// DeleteRange removes all log entries within the specified min and max range inclusive.
// If min > max, this is a no-op and returns nil.
func (b *BoltStore) DeleteRange(min, max uint64) error {
	if min > max {
		return nil
	}

	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(logsBucket)
		for i := min; i <= max; i++ {
			key := uint64ToBytes(i)
			if err := bucket.Delete(key); err != nil {
				return fmt.Errorf("failed to delete log entry at index %d: %w", i, err)
			}
		}
		return nil
	})
}

// SetCompactedIndex sets the compacted index and persists it to stable storage.
// This should be called after log compaction to track which entries have been compacted.
func (b *BoltStore) SetCompactedIndex(index uint64) error {
	if err := b.SetUint64(keyCompactedIndex, index); err != nil {
		return fmt.Errorf("failed to persist compacted index: %w", err)
	}
	b.compactedIndex = index
	return nil
}

// GetCompactedIndex returns the current compacted index.
// Returns 0 if no compaction has occurred.
func (b *BoltStore) GetCompactedIndex() uint64 {
	return b.compactedIndex
}

// ============================================================================
// StableStore Interface Implementation
// ============================================================================

// Set stores a key-value pair in the stable bucket.
// The value is stored as raw bytes.
func (b *BoltStore) Set(key []byte, val []byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(stableBucket)
		if err := bucket.Put(key, val); err != nil {
			return fmt.Errorf("failed to set key: %w", err)
		}
		return nil
	})
}

// Get retrieves a value by key from the stable bucket.
// Returns an empty byte slice if the key does not exist.
func (b *BoltStore) Get(key []byte) ([]byte, error) {
	var val []byte
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(stableBucket)
		v := bucket.Get(key)
		if v == nil {
			val = []byte{}
			return nil
		}
		// Make a copy since BoltDB values are only valid within the transaction
		val = make([]byte, len(v))
		copy(val, v)
		return nil
	})
	return val, err
}

// SetUint64 stores a uint64 value encoded as big-endian bytes.
func (b *BoltStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

// GetUint64 retrieves a uint64 value by key from the stable bucket.
// Returns 0 if the key does not exist.
func (b *BoltStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	return bytesToUint64(val), nil
}
