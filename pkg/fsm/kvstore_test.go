// Package fsm provides unit tests for the KVStore implementation.
package fsm

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
)

// TestApply_MalformedJSON verifies that Apply returns an error for malformed JSON.
func TestApply_MalformedJSON(t *testing.T) {
	store := NewKVStore()

	testCases := []struct {
		name  string
		input []byte
	}{
		{"empty bytes", []byte{}},
		{"invalid JSON", []byte("not json")},
		{"incomplete JSON", []byte(`{"op": "set"`)},
		{"array instead of object", []byte(`["set", "key", "value"]`)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := store.Apply(tc.input)
			if result != ErrInvalidJSON {
				t.Errorf("Apply(%q) = %v, want ErrInvalidJSON", tc.input, result)
			}
		})
	}
}

// TestApply_UnknownOperation verifies that Apply ignores unknown operations.
func TestApply_UnknownOperation(t *testing.T) {
	store := NewKVStore()

	// Set initial state
	setCmd := Command{Op: "set", Key: "existing", Value: "value1"}
	setCmdBytes, _ := json.Marshal(setCmd)
	store.Apply(setCmdBytes)

	testCases := []struct {
		name string
		op   string
	}{
		{"get operation", "get"},
		{"empty operation", ""},
		{"random operation", "foobar"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := Command{Op: tc.op, Key: "testkey", Value: "testvalue"}
			cmdBytes, _ := json.Marshal(cmd)

			result := store.Apply(cmdBytes)
			if result != nil {
				t.Errorf("Apply with op=%q returned %v, want nil", tc.op, result)
			}

			// Verify state unchanged - existing key still has original value
			val, ok := store.Get("existing")
			if !ok || val != "value1" {
				t.Errorf("State changed after unknown op: got (%q, %v), want (\"value1\", true)", val, ok)
			}

			// Verify unknown op didn't add a new key
			_, exists := store.Get("testkey")
			if exists {
				t.Errorf("Unknown op %q added key \"testkey\"", tc.op)
			}
		})
	}
}

// TestGet_NonExistingKey verifies that Get returns empty string and false for non-existing keys.
func TestGet_NonExistingKey(t *testing.T) {
	store := NewKVStore()

	// Test on empty store
	val, ok := store.Get("nonexistent")
	if ok {
		t.Errorf("Get(\"nonexistent\") on empty store returned ok=true, want false")
	}
	if val != "" {
		t.Errorf("Get(\"nonexistent\") on empty store returned %q, want empty string", val)
	}

	// Add some data and test for a different key
	cmd := Command{Op: "set", Key: "existing", Value: "value"}
	cmdBytes, _ := json.Marshal(cmd)
	store.Apply(cmdBytes)

	val, ok = store.Get("other-key")
	if ok {
		t.Errorf("Get(\"other-key\") returned ok=true, want false")
	}
	if val != "" {
		t.Errorf("Get(\"other-key\") returned %q, want empty string", val)
	}
}

// TestSnapshotRestoreIntegration verifies the complete snapshot/restore workflow.
func TestSnapshotRestoreIntegration(t *testing.T) {
	store := NewKVStore()

	// Step 1: Apply "SET A 1"
	cmd := Command{Op: "set", Key: "A", Value: "1"}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("Failed to marshal command: %v", err)
	}
	result := store.Apply(cmdBytes)
	if result != nil {
		t.Fatalf("Apply returned error: %v", result)
	}

	// Verify A is set
	val, ok := store.Get("A")
	if !ok || val != "1" {
		t.Fatalf("After Apply: Get(\"A\") = (%q, %v), want (\"1\", true)", val, ok)
	}

	// Step 2: Take snapshot
	snapshot, err := store.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	// Read snapshot data for later restore
	snapshotData, err := io.ReadAll(snapshot)
	if err != nil {
		t.Fatalf("Failed to read snapshot: %v", err)
	}
	snapshot.Close()

	// Step 3: Clear map by creating a new store (simulating state loss)
	store2 := NewKVStore()

	// Verify the new store doesn't have key A
	_, ok = store2.Get("A")
	if ok {
		t.Fatal("New store should not have key A")
	}

	// Step 4: Restore from snapshot
	reader := io.NopCloser(bytes.NewReader(snapshotData))
	err = store2.Restore(reader)
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Step 5: Verify "A" is still "1"
	val, ok = store2.Get("A")
	if !ok {
		t.Error("After Restore: key \"A\" not found")
	}
	if val != "1" {
		t.Errorf("After Restore: Get(\"A\") = %q, want \"1\"", val)
	}
}

// TestApply_Delete verifies that delete operation removes keys.
func TestApply_Delete(t *testing.T) {
	store := NewKVStore()

	// Set a key first
	setCmd := Command{Op: "set", Key: "mykey", Value: "myvalue"}
	setCmdBytes, _ := json.Marshal(setCmd)
	store.Apply(setCmdBytes)

	// Verify key exists
	val, ok := store.Get("mykey")
	if !ok || val != "myvalue" {
		t.Fatalf("Setup failed: Get(\"mykey\") = (%q, %v), want (\"myvalue\", true)", val, ok)
	}

	// Delete the key
	delCmd := Command{Op: "delete", Key: "mykey"}
	delCmdBytes, _ := json.Marshal(delCmd)
	result := store.Apply(delCmdBytes)
	if result != nil {
		t.Errorf("Apply delete returned %v, want nil", result)
	}

	// Verify key is gone
	_, ok = store.Get("mykey")
	if ok {
		t.Error("Key still exists after delete")
	}

	// Deleting non-existent key should be a no-op
	result = store.Apply(delCmdBytes)
	if result != nil {
		t.Errorf("Apply delete on non-existent key returned %v, want nil", result)
	}
}

// TestSize verifies that Size returns the correct count.
func TestSize(t *testing.T) {
	store := NewKVStore()

	if got := store.Size(); got != 0 {
		t.Errorf("Empty store Size() = %d, want 0", got)
	}

	// Add keys
	for i, key := range []string{"a", "b", "c"} {
		cmd := Command{Op: "set", Key: key, Value: "val"}
		cmdBytes, _ := json.Marshal(cmd)
		store.Apply(cmdBytes)

		if got := store.Size(); got != i+1 {
			t.Errorf("After adding %d keys, Size() = %d, want %d", i+1, got, i+1)
		}
	}

	// Delete one
	delCmd := Command{Op: "delete", Key: "b"}
	delCmdBytes, _ := json.Marshal(delCmd)
	store.Apply(delCmdBytes)

	if got := store.Size(); got != 2 {
		t.Errorf("After delete, Size() = %d, want 2", got)
	}
}

// TestKeys verifies that Keys returns all keys.
func TestKeys(t *testing.T) {
	store := NewKVStore()

	if keys := store.Keys(); len(keys) != 0 {
		t.Errorf("Empty store Keys() = %v, want empty slice", keys)
	}

	// Add keys
	expected := map[string]bool{"alpha": true, "beta": true, "gamma": true}
	for key := range expected {
		cmd := Command{Op: "set", Key: key, Value: "val"}
		cmdBytes, _ := json.Marshal(cmd)
		store.Apply(cmdBytes)
	}

	keys := store.Keys()
	if len(keys) != 3 {
		t.Errorf("Keys() returned %d keys, want 3", len(keys))
	}

	for _, k := range keys {
		if !expected[k] {
			t.Errorf("Unexpected key %q in Keys() result", k)
		}
	}
}
