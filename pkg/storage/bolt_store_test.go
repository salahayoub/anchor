// Package storage provides unit tests for the BoltStore implementation.
package storage

import (
	"testing"

	"github.com/salahayoub/anchor/api"
)

// TestEmptyStoreFirstIndex verifies that FirstIndex returns 0 for an empty store.
func TestEmptyStoreFirstIndex(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"
	store, err := NewBoltStore(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	firstIndex, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex returned error: %v", err)
	}
	if firstIndex != 0 {
		t.Errorf("Expected FirstIndex to return 0 for empty store, got %d", firstIndex)
	}
}

// TestEmptyStoreLastIndex verifies that LastIndex returns 0 for an empty store.
func TestEmptyStoreLastIndex(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"
	store, err := NewBoltStore(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	lastIndex, err := store.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex returned error: %v", err)
	}
	if lastIndex != 0 {
		t.Errorf("Expected LastIndex to return 0 for empty store, got %d", lastIndex)
	}
}

// TestGetReturnsEmptyForMissingKey verifies that Get returns an empty slice for non-existing keys.
func TestGetReturnsEmptyForMissingKey(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"
	store, err := NewBoltStore(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	val, err := store.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if len(val) != 0 {
		t.Errorf("Expected Get to return empty slice for missing key, got %v", val)
	}
}

// TestGetUint64ReturnsZeroForMissingKey verifies that GetUint64 returns 0 for non-existing keys.
func TestGetUint64ReturnsZeroForMissingKey(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"
	store, err := NewBoltStore(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	val, err := store.GetUint64([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("GetUint64 returned error: %v", err)
	}
	if val != 0 {
		t.Errorf("Expected GetUint64 to return 0 for missing key, got %d", val)
	}
}

// TestDatabaseCloseAndReopenPersistence verifies that data persists after closing and reopening.
func TestDatabaseCloseAndReopenPersistence(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	// Create store and add data
	store, err := NewBoltStore(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Store log entries
	entries := []*api.LogEntry{
		{Index: 1, Term: 1, Type: api.LogType_LOG_COMMAND, Data: []byte("cmd1")},
		{Index: 2, Term: 1, Type: api.LogType_LOG_COMMAND, Data: []byte("cmd2")},
		{Index: 3, Term: 2, Type: api.LogType_LOG_NOOP, Data: nil},
	}
	if err := store.StoreLogs(entries); err != nil {
		store.Close()
		t.Fatalf("Failed to store logs: %v", err)
	}

	// Store stable state
	if err := store.Set([]byte("votedFor"), []byte("node1")); err != nil {
		store.Close()
		t.Fatalf("Failed to set votedFor: %v", err)
	}
	if err := store.SetUint64([]byte("currentTerm"), 2); err != nil {
		store.Close()
		t.Fatalf("Failed to set currentTerm: %v", err)
	}

	// Close the store
	if err := store.Close(); err != nil {
		t.Fatalf("Failed to close store: %v", err)
	}

	// Reopen the store
	store, err = NewBoltStore(tmpFile)
	if err != nil {
		t.Fatalf("Failed to reopen store: %v", err)
	}
	defer store.Close()

	// Verify log entries persist
	firstIndex, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex returned error: %v", err)
	}
	if firstIndex != 1 {
		t.Errorf("Expected FirstIndex to be 1, got %d", firstIndex)
	}

	lastIndex, err := store.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex returned error: %v", err)
	}
	if lastIndex != 3 {
		t.Errorf("Expected LastIndex to be 3, got %d", lastIndex)
	}

	for _, expected := range entries {
		retrieved, err := store.GetLog(expected.Index)
		if err != nil {
			t.Fatalf("Failed to get log at index %d: %v", expected.Index, err)
		}
		if retrieved.Index != expected.Index {
			t.Errorf("Index mismatch: expected %d, got %d", expected.Index, retrieved.Index)
		}
		if retrieved.Term != expected.Term {
			t.Errorf("Term mismatch at index %d: expected %d, got %d", expected.Index, expected.Term, retrieved.Term)
		}
		if retrieved.Type != expected.Type {
			t.Errorf("Type mismatch at index %d: expected %v, got %v", expected.Index, expected.Type, retrieved.Type)
		}
	}

	// Verify stable state persists
	votedFor, err := store.Get([]byte("votedFor"))
	if err != nil {
		t.Fatalf("Failed to get votedFor: %v", err)
	}
	if string(votedFor) != "node1" {
		t.Errorf("Expected votedFor to be 'node1', got '%s'", string(votedFor))
	}

	currentTerm, err := store.GetUint64([]byte("currentTerm"))
	if err != nil {
		t.Fatalf("Failed to get currentTerm: %v", err)
	}
	if currentTerm != 2 {
		t.Errorf("Expected currentTerm to be 2, got %d", currentTerm)
	}
}

// TestEmptyStoreGetLogReturnsError verifies GetLog returns error for empty store.
func TestEmptyStoreGetLogReturnsError(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"
	store, err := NewBoltStore(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	_, err = store.GetLog(1)
	if err != ErrLogNotFound {
		t.Errorf("Expected ErrLogNotFound for empty store, got %v", err)
	}
}

// TestGetLogIndex0ReturnsError verifies GetLog returns error for index 0.
func TestGetLogIndex0ReturnsError(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"
	store, err := NewBoltStore(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Store some entries
	entries := []*api.LogEntry{
		{Index: 1, Term: 1, Type: api.LogType_LOG_COMMAND, Data: []byte("cmd")},
	}
	if err := store.StoreLogs(entries); err != nil {
		t.Fatalf("Failed to store logs: %v", err)
	}

	// GetLog(0) should return error
	_, err = store.GetLog(0)
	if err != ErrLogNotFound {
		t.Errorf("Expected ErrLogNotFound for index 0, got %v", err)
	}
}

// TestStoreLogsEmptySlice verifies StoreLogs handles empty slice gracefully.
func TestStoreLogsEmptySlice(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"
	store, err := NewBoltStore(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// StoreLogs with empty slice should be no-op
	err = store.StoreLogs([]*api.LogEntry{})
	if err != nil {
		t.Errorf("StoreLogs with empty slice returned error: %v", err)
	}

	// Store should still be empty
	firstIndex, _ := store.FirstIndex()
	lastIndex, _ := store.LastIndex()
	if firstIndex != 0 || lastIndex != 0 {
		t.Errorf("Expected empty store after StoreLogs([]), got first=%d, last=%d", firstIndex, lastIndex)
	}
}

// TestDeleteRangeMinGreaterThanMax verifies DeleteRange is no-op when min > max.
func TestDeleteRangeMinGreaterThanMax(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"
	store, err := NewBoltStore(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Store some entries
	entries := []*api.LogEntry{
		{Index: 1, Term: 1, Type: api.LogType_LOG_COMMAND, Data: []byte("cmd1")},
		{Index: 2, Term: 1, Type: api.LogType_LOG_COMMAND, Data: []byte("cmd2")},
	}
	if err := store.StoreLogs(entries); err != nil {
		t.Fatalf("Failed to store logs: %v", err)
	}

	// DeleteRange with min > max should be no-op
	err = store.DeleteRange(10, 1)
	if err != nil {
		t.Errorf("DeleteRange with min > max returned error: %v", err)
	}

	// All entries should still exist
	for _, entry := range entries {
		_, err := store.GetLog(entry.Index)
		if err != nil {
			t.Errorf("Entry at index %d should still exist after no-op DeleteRange", entry.Index)
		}
	}
}
