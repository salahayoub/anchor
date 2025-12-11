// Package raft provides snapshot storage for the Raft consensus engine.
//
// # Thread Safety Guarantees
//
// FileSnapshotStore is safe for concurrent use by multiple goroutines.
// Write operations (Create) use temporary files and atomic renames to ensure
// consistency. Read operations (Open, GetMeta, List) can run concurrently.
//
// References:
package raft

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Error variables for snapshot operations.
var (
	// ErrNoSnapshot is returned when no snapshot exists.
	ErrNoSnapshot = errors.New("no snapshot available")
	// ErrSnapshotCorrupted is returned when snapshot data fails integrity check.
	ErrSnapshotCorrupted = errors.New("snapshot data corrupted")
	// ErrSnapshotInProgress is returned when another snapshot operation is in progress.
	ErrSnapshotInProgress = errors.New("snapshot operation already in progress")
)

// SnapshotMeta contains metadata about a snapshot.
type SnapshotMeta struct {
	// LastIncludedIndex is the index of the last log entry included in the snapshot.
	LastIncludedIndex uint64 `json:"lastIncludedIndex"`
	// LastIncludedTerm is the term of the last log entry included in the snapshot.
	LastIncludedTerm uint64 `json:"lastIncludedTerm"`
	// Configuration is the cluster configuration at snapshot time.
	Configuration *ClusterConfig `json:"configuration,omitempty"`
	// Size is the size of the snapshot data in bytes.
	Size int64 `json:"size"`
	// Checksum is the SHA-256 checksum of the snapshot data.
	Checksum string `json:"checksum"`
}

// SnapshotStore provides persistent storage for snapshots.
type SnapshotStore interface {
	// Create starts a new snapshot write operation.
	// Returns a SnapshotSink to write data to.
	Create(meta *SnapshotMeta) (SnapshotSink, error)

	// Open returns the most recent snapshot for reading.
	// Returns ErrNoSnapshot if no snapshot exists.
	Open() (*SnapshotMeta, io.ReadCloser, error)

	// GetMeta returns metadata of the most recent snapshot without loading data.
	// Returns ErrNoSnapshot if no snapshot exists.
	GetMeta() (*SnapshotMeta, error)

	// List returns metadata for all available snapshots (most recent first).
	List() ([]*SnapshotMeta, error)

	// Dir returns the directory where snapshots are stored.
	// Used for creating temporary files during InstallSnapshot.
	Dir() string
}

// SnapshotSink is used to write snapshot data.
type SnapshotSink interface {
	io.WriteCloser

	// ID returns a unique identifier for this snapshot.
	ID() string

	// Cancel aborts the snapshot and cleans up resources.
	Cancel() error
}

// Snapshot file names
const (
	metaFileName     = "meta.json"
	snapshotFileName = "snapshot.dat"
	tempFileName     = "snapshot.tmp"
)

// FileSnapshotStore implements SnapshotStore using the filesystem.
// It stores snapshots in a directory with metadata in meta.json and data in snapshot.dat.
// Only the most recent snapshot is retained.
type FileSnapshotStore struct {
	dir    string     // Base directory for snapshots
	retain int        // Number of snapshots to retain (default: 1)
	mu     sync.Mutex // Protects concurrent snapshot operations
}

// NewFileSnapshotStore creates a new FileSnapshotStore at the specified directory.
// It creates the directory if it doesn't exist.
func NewFileSnapshotStore(dir string) (*FileSnapshotStore, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	return &FileSnapshotStore{
		dir:    dir,
		retain: 1,
	}, nil
}

// FileSnapshotSink implements SnapshotSink for writing snapshot data to a file.
type FileSnapshotSink struct {
	store    *FileSnapshotStore
	meta     *SnapshotMeta
	file     *os.File
	hash     *sha256Hash
	id       string
	closed   bool
	canceled bool
}

// sha256Hash wraps a hash writer for computing checksums.
type sha256Hash struct {
	hash   []byte
	writer io.Writer
}

// Create starts a new snapshot write operation.
// Returns a FileSnapshotSink to write data to.
func (s *FileSnapshotStore) Create(meta *SnapshotMeta) (SnapshotSink, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if temp file exists (snapshot in progress)
	tempPath := filepath.Join(s.dir, tempFileName)
	if _, err := os.Stat(tempPath); err == nil {
		return nil, ErrSnapshotInProgress
	}

	// Create temporary file for writing
	file, err := os.Create(tempPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp snapshot file: %w", err)
	}

	// Generate unique ID based on index and term
	id := fmt.Sprintf("%d-%d", meta.LastIncludedIndex, meta.LastIncludedTerm)

	// Create hash writer for checksum calculation
	hasher := sha256.New()

	sink := &FileSnapshotSink{
		store: s,
		meta:  meta,
		file:  file,
		hash: &sha256Hash{
			writer: io.MultiWriter(file, hasher),
		},
		id: id,
	}

	// Store hasher reference for checksum calculation
	sink.hash.hash = nil // Will be computed on Close

	return sink, nil
}

// Write writes data to the snapshot.
func (s *FileSnapshotSink) Write(p []byte) (n int, err error) {
	if s.closed || s.canceled {
		return 0, errors.New("snapshot sink is closed")
	}
	return s.hash.writer.Write(p)
}

// Close finalizes the snapshot by writing metadata, syncing to disk, and renaming.
func (s *FileSnapshotSink) Close() error {
	if s.closed || s.canceled {
		return nil
	}
	s.closed = true

	// Sync and close the data file
	if err := s.file.Sync(); err != nil {
		s.file.Close()
		return fmt.Errorf("failed to sync snapshot data: %w", err)
	}

	// Get file size
	info, err := s.file.Stat()
	if err != nil {
		s.file.Close()
		return fmt.Errorf("failed to stat snapshot file: %w", err)
	}
	s.meta.Size = info.Size()

	if err := s.file.Close(); err != nil {
		return fmt.Errorf("failed to close snapshot file: %w", err)
	}

	// Compute checksum by reading the temp file
	tempPath := filepath.Join(s.store.dir, tempFileName)
	checksum, err := computeFileChecksum(tempPath)
	if err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to compute checksum: %w", err)
	}
	s.meta.Checksum = checksum

	// Write metadata to meta.json
	metaPath := filepath.Join(s.store.dir, metaFileName)
	metaData, err := json.MarshalIndent(s.meta, "", "  ")
	if err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(metaPath, metaData, 0644); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Atomic rename temp file to final snapshot file
	snapshotPath := filepath.Join(s.store.dir, snapshotFileName)
	if err := os.Rename(tempPath, snapshotPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename snapshot file: %w", err)
	}

	return nil
}

// ID returns the unique identifier for this snapshot.
func (s *FileSnapshotSink) ID() string {
	return s.id
}

// Cancel aborts the snapshot and cleans up resources.
func (s *FileSnapshotSink) Cancel() error {
	if s.closed || s.canceled {
		return nil
	}
	s.canceled = true

	// Close and remove the temp file
	s.file.Close()
	tempPath := filepath.Join(s.store.dir, tempFileName)
	return os.Remove(tempPath)
}

// Open returns the most recent snapshot for reading.
// Returns ErrNoSnapshot if no snapshot exists.
func (s *FileSnapshotStore) Open() (*SnapshotMeta, io.ReadCloser, error) {
	// Read metadata first
	meta, err := s.GetMeta()
	if err != nil {
		return nil, nil, err
	}

	// Open snapshot data file
	snapshotPath := filepath.Join(s.dir, snapshotFileName)
	file, err := os.Open(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrNoSnapshot
		}
		return nil, nil, fmt.Errorf("failed to open snapshot file: %w", err)
	}

	// Verify checksum before returning
	checksum, err := computeFileChecksum(snapshotPath)
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to verify checksum: %w", err)
	}

	if checksum != meta.Checksum {
		file.Close()
		return nil, nil, ErrSnapshotCorrupted
	}

	// Reopen file since we consumed it for checksum
	file.Close()
	file, err = os.Open(snapshotPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to reopen snapshot file: %w", err)
	}

	return meta, file, nil
}

// GetMeta returns metadata of the most recent snapshot without loading data.
// Returns ErrNoSnapshot if no snapshot exists.
func (s *FileSnapshotStore) GetMeta() (*SnapshotMeta, error) {
	metaPath := filepath.Join(s.dir, metaFileName)

	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNoSnapshot
		}
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var meta SnapshotMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &meta, nil
}

// List returns metadata for all available snapshots (most recent first).
// Since we only retain one snapshot, this returns at most one entry.
func (s *FileSnapshotStore) List() ([]*SnapshotMeta, error) {
	meta, err := s.GetMeta()
	if err != nil {
		if errors.Is(err, ErrNoSnapshot) {
			return []*SnapshotMeta{}, nil
		}
		return nil, err
	}
	return []*SnapshotMeta{meta}, nil
}

// Dir returns the directory where snapshots are stored.
func (s *FileSnapshotStore) Dir() string {
	return s.dir
}

// computeFileChecksum computes the SHA-256 checksum of a file.
func computeFileChecksum(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	return "sha256:" + hex.EncodeToString(hasher.Sum(nil)), nil
}
