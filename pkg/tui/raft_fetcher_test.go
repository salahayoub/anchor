// Package tui provides unit tests for the RaftDataFetcher implementation.
package tui

import (
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/salahayoub/anchor/api"
	"github.com/salahayoub/anchor/pkg/fsm"
	"github.com/salahayoub/anchor/pkg/raft"
	"github.com/salahayoub/anchor/pkg/transport"
)

// mockStableStore is a simple in-memory implementation of StableStore for testing.
type mockStableStore struct {
	mu     sync.RWMutex
	data   map[string][]byte
	uint64 map[string]uint64
}

func newMockStableStore() *mockStableStore {
	return &mockStableStore{
		data:   make(map[string][]byte),
		uint64: make(map[string]uint64),
	}
}

func (m *mockStableStore) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if val, ok := m.data[string(key)]; ok {
		return val, nil
	}
	return []byte{}, nil
}

func (m *mockStableStore) Set(key []byte, val []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[string(key)] = val
	return nil
}

func (m *mockStableStore) GetUint64(key []byte) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.uint64[string(key)], nil
}

func (m *mockStableStore) SetUint64(key []byte, val uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uint64[string(key)] = val
	return nil
}

// mockLogStore is a simple in-memory implementation of LogStore for testing.
type mockLogStore struct {
	mu   sync.RWMutex
	logs map[uint64]*api.LogEntry
}

func newMockLogStore() *mockLogStore {
	return &mockLogStore{
		logs: make(map[uint64]*api.LogEntry),
	}
}

func (m *mockLogStore) FirstIndex() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.logs) == 0 {
		return 0, nil
	}
	min := ^uint64(0)
	for idx := range m.logs {
		if idx < min {
			min = idx
		}
	}
	return min, nil
}

func (m *mockLogStore) LastIndex() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var max uint64
	for idx := range m.logs {
		if idx > max {
			max = idx
		}
	}
	return max, nil
}

func (m *mockLogStore) GetLog(index uint64) (*api.LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if entry, ok := m.logs[index]; ok {
		return entry, nil
	}
	return nil, raft.ErrLogInconsistent
}

func (m *mockLogStore) StoreLogs(logs []*api.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, log := range logs {
		m.logs[log.Index] = log
	}
	return nil
}

func (m *mockLogStore) DeleteRange(min, max uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := min; i <= max; i++ {
		delete(m.logs, i)
	}
	return nil
}

// mockStateMachine is a simple implementation of StateMachine for testing.
type mockStateMachine struct {
	mu   sync.RWMutex
	data []byte
}

func newMockStateMachine() *mockStateMachine {
	return &mockStateMachine{}
}

func (m *mockStateMachine) Apply(logBytes []byte) interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = append(m.data, logBytes...)
	return nil
}

func (m *mockStateMachine) Snapshot() (io.ReadCloser, error) {
	return nil, nil
}

func (m *mockStateMachine) Restore(rc io.ReadCloser) error {
	return nil
}

// mockTransport is a simple implementation of Transport for testing.
type mockTransport struct {
	mu       sync.RWMutex
	addr     string
	consumer chan transport.RPC
	closed   bool
}

func newMockTransport(addr string) *mockTransport {
	return &mockTransport{
		addr:     addr,
		consumer: make(chan transport.RPC, 100),
	}
}

func (m *mockTransport) Consumer() <-chan transport.RPC {
	return m.consumer
}

func (m *mockTransport) LocalAddr() string {
	return m.addr
}

func (m *mockTransport) SendRequestVote(target string, req *api.VoteRequest) (*api.VoteResponse, error) {
	return &api.VoteResponse{Term: req.Term, VoteGranted: false}, nil
}

func (m *mockTransport) SendAppendEntries(target string, req *api.AppendEntriesRequest) (*api.AppendEntriesResponse, error) {
	return &api.AppendEntriesResponse{Term: req.Term, Success: true, LastLogIndex: req.PrevLogIndex + uint64(len(req.Entries))}, nil
}

func (m *mockTransport) SendInstallSnapshot(target string, req *api.InstallSnapshotRequest) (*api.InstallSnapshotResponse, error) {
	return nil, nil
}

func (m *mockTransport) SendJoinCluster(target string, req *api.JoinClusterRequest) (*api.JoinClusterResponse, error) {
	return &api.JoinClusterResponse{Success: true}, nil
}

func (m *mockTransport) SendRemoveServer(target string, req *api.RemoveServerRequest) (*api.RemoveServerResponse, error) {
	return &api.RemoveServerResponse{Success: true}, nil
}

func (m *mockTransport) SendRead(target string, req *api.ReadRequest) (*api.ReadResponse, error) {
	return &api.ReadResponse{Found: false}, nil
}

func (m *mockTransport) Connect(peerAddr string) error {
	return nil
}

func (m *mockTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.consumer)
	}
	return nil
}

// createTestRaftNode creates a Raft node for testing purposes.
func createTestRaftNode(t *testing.T) (*raft.Raft, *mockLogStore, *fsm.KVStore) {
	t.Helper()

	stableStore := newMockStableStore()
	logStore := newMockLogStore()
	kvStore := fsm.NewKVStore()
	trans := newMockTransport("node1")

	config := raft.Config{
		ID:               "node1",
		Peers:            []string{"node2", "node3"},
		ElectionTimeout:  500 * time.Millisecond,
		HeartbeatTimeout: 250 * time.Millisecond,
	}

	r, err := raft.NewRaft(config, logStore, stableStore, kvStore, trans, nil)
	if err != nil {
		t.Fatalf("Failed to create Raft node: %v", err)
	}

	return r, logStore, kvStore
}

// TestRaftDataFetcher_FetchClusterState tests that FetchClusterState returns correct node roles and metrics.
func TestRaftDataFetcher_FetchClusterState(t *testing.T) {
	r, logStore, kvStore := createTestRaftNode(t)

	fetcher := NewRaftDataFetcher(r, kvStore, logStore)

	state, err := fetcher.FetchClusterState()
	if err != nil {
		t.Fatalf("FetchClusterState failed: %v", err)
	}

	// Verify local node ID is set
	if state.LocalNodeID == "" {
		t.Error("Expected LocalNodeID to be set")
	}

	// Verify local role is set (should be Follower initially)
	if state.LocalRole == "" {
		t.Error("Expected LocalRole to be set")
	}
	if state.LocalRole != "Follower" {
		t.Errorf("Expected LocalRole to be 'Follower', got %q", state.LocalRole)
	}

	// Verify nodes list is populated
	if len(state.Nodes) == 0 {
		t.Error("Expected Nodes list to be populated")
	}

	// Verify LastUpdated is recent
	if time.Since(state.LastUpdated) > time.Second {
		t.Error("Expected LastUpdated to be recent")
	}
}

// TestRaftDataFetcher_FetchClusterState_Disconnected tests that FetchClusterState returns error when disconnected.
func TestRaftDataFetcher_FetchClusterState_Disconnected(t *testing.T) {
	r, logStore, kvStore := createTestRaftNode(t)

	fetcher := NewRaftDataFetcher(r, kvStore, logStore)
	fetcher.SetConnected(false)

	_, err := fetcher.FetchClusterState()
	if err != ErrNotConnected {
		t.Errorf("Expected ErrNotConnected, got %v", err)
	}
}

// TestRaftDataFetcher_FetchRecentLogs tests that FetchRecentLogs returns log entries.
func TestRaftDataFetcher_FetchRecentLogs(t *testing.T) {
	r, logStore, kvStore := createTestRaftNode(t)

	// Add some log entries
	cmd := fsm.Command{Op: "set", Key: "testkey", Value: "testvalue"}
	cmdBytes, _ := json.Marshal(cmd)

	entries := []*api.LogEntry{
		{Index: 1, Term: 1, Type: api.LogType_LOG_COMMAND, Data: cmdBytes},
		{Index: 2, Term: 1, Type: api.LogType_LOG_COMMAND, Data: cmdBytes},
		{Index: 3, Term: 1, Type: api.LogType_LOG_COMMAND, Data: cmdBytes},
	}
	if err := logStore.StoreLogs(entries); err != nil {
		t.Fatalf("Failed to store logs: %v", err)
	}

	fetcher := NewRaftDataFetcher(r, kvStore, logStore)

	logs, err := fetcher.FetchRecentLogs(10)
	if err != nil {
		t.Fatalf("FetchRecentLogs failed: %v", err)
	}

	if len(logs) != 3 {
		t.Errorf("Expected 3 log entries, got %d", len(logs))
	}

	// Verify log entry fields
	for _, log := range logs {
		if log.Index == 0 {
			t.Error("Expected log Index to be set")
		}
		if log.Term == 0 {
			t.Error("Expected log Term to be set")
		}
		if log.Operation == "" {
			t.Error("Expected log Operation to be set")
		}
	}
}

// TestRaftDataFetcher_FetchRecentLogs_Empty tests that FetchRecentLogs handles empty log.
func TestRaftDataFetcher_FetchRecentLogs_Empty(t *testing.T) {
	r, logStore, kvStore := createTestRaftNode(t)

	fetcher := NewRaftDataFetcher(r, kvStore, logStore)

	logs, err := fetcher.FetchRecentLogs(10)
	if err != nil {
		t.Fatalf("FetchRecentLogs failed: %v", err)
	}

	if len(logs) != 0 {
		t.Errorf("Expected 0 log entries for empty log, got %d", len(logs))
	}
}

// TestRaftDataFetcher_FetchRecentLogs_LimitCount tests that FetchRecentLogs respects count limit.
func TestRaftDataFetcher_FetchRecentLogs_LimitCount(t *testing.T) {
	r, logStore, kvStore := createTestRaftNode(t)

	// Add 15 log entries
	entries := make([]*api.LogEntry, 15)
	for i := 0; i < 15; i++ {
		entries[i] = &api.LogEntry{
			Index: uint64(i + 1),
			Term:  1,
			Type:  api.LogType_LOG_COMMAND,
			Data:  []byte(`{"op":"set","key":"k","value":"v"}`),
		}
	}
	if err := logStore.StoreLogs(entries); err != nil {
		t.Fatalf("Failed to store logs: %v", err)
	}

	fetcher := NewRaftDataFetcher(r, kvStore, logStore)

	// Request only 5 entries
	logs, err := fetcher.FetchRecentLogs(5)
	if err != nil {
		t.Fatalf("FetchRecentLogs failed: %v", err)
	}

	if len(logs) != 5 {
		t.Errorf("Expected 5 log entries, got %d", len(logs))
	}

	// Verify we got the most recent entries (11-15)
	if logs[0].Index != 11 {
		t.Errorf("Expected first entry index to be 11, got %d", logs[0].Index)
	}
	if logs[4].Index != 15 {
		t.Errorf("Expected last entry index to be 15, got %d", logs[4].Index)
	}
}

// TestRaftDataFetcher_ExecuteGet tests that ExecuteGet retrieves values from KVStore.
func TestRaftDataFetcher_ExecuteGet(t *testing.T) {
	r, logStore, kvStore := createTestRaftNode(t)

	// Set a value in the KVStore directly
	cmd := fsm.Command{Op: "set", Key: "mykey", Value: "myvalue"}
	cmdBytes, _ := json.Marshal(cmd)
	kvStore.Apply(cmdBytes)

	fetcher := NewRaftDataFetcher(r, kvStore, logStore)

	value, err := fetcher.ExecuteGet("mykey")
	if err != nil {
		t.Fatalf("ExecuteGet failed: %v", err)
	}

	if value != "myvalue" {
		t.Errorf("Expected value 'myvalue', got %q", value)
	}
}

// TestRaftDataFetcher_ExecuteGet_NotFound tests that ExecuteGet returns error for missing key.
func TestRaftDataFetcher_ExecuteGet_NotFound(t *testing.T) {
	r, logStore, kvStore := createTestRaftNode(t)

	fetcher := NewRaftDataFetcher(r, kvStore, logStore)

	_, err := fetcher.ExecuteGet("nonexistent")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

// TestRaftDataFetcher_ExecuteGet_Disconnected tests that ExecuteGet returns error when disconnected.
func TestRaftDataFetcher_ExecuteGet_Disconnected(t *testing.T) {
	r, logStore, kvStore := createTestRaftNode(t)

	fetcher := NewRaftDataFetcher(r, kvStore, logStore)
	fetcher.SetConnected(false)

	_, err := fetcher.ExecuteGet("anykey")
	if err != ErrNotConnected {
		t.Errorf("Expected ErrNotConnected, got %v", err)
	}
}

// TestRaftDataFetcher_ExecutePut_NotLeader tests that ExecutePut returns error when not leader.
func TestRaftDataFetcher_ExecutePut_NotLeader(t *testing.T) {
	r, logStore, kvStore := createTestRaftNode(t)

	fetcher := NewRaftDataFetcher(r, kvStore, logStore)

	// Node is a follower, so ExecutePut should fail
	err := fetcher.ExecutePut("key", "value")
	if !errors.Is(err, raft.ErrNotLeader) {
		t.Errorf("Expected ErrNotLeader, got %v", err)
	}
}

// TestRaftDataFetcher_ExecutePut_Disconnected tests that ExecutePut returns error when disconnected.
func TestRaftDataFetcher_ExecutePut_Disconnected(t *testing.T) {
	r, logStore, kvStore := createTestRaftNode(t)

	fetcher := NewRaftDataFetcher(r, kvStore, logStore)
	fetcher.SetConnected(false)

	err := fetcher.ExecutePut("key", "value")
	if err != ErrNotConnected {
		t.Errorf("Expected ErrNotConnected, got %v", err)
	}
}

// TestRaftDataFetcher_IsConnected tests the IsConnected method.
func TestRaftDataFetcher_IsConnected(t *testing.T) {
	r, logStore, kvStore := createTestRaftNode(t)

	fetcher := NewRaftDataFetcher(r, kvStore, logStore)

	// Initially connected
	if !fetcher.IsConnected() {
		t.Error("Expected IsConnected to return true initially")
	}

	// Set disconnected
	fetcher.SetConnected(false)
	if fetcher.IsConnected() {
		t.Error("Expected IsConnected to return false after SetConnected(false)")
	}

	// Set connected again
	fetcher.SetConnected(true)
	if !fetcher.IsConnected() {
		t.Error("Expected IsConnected to return true after SetConnected(true)")
	}
}

// TestRaftDataFetcher_Reconnect tests the Reconnect method.
func TestRaftDataFetcher_Reconnect(t *testing.T) {
	r, logStore, kvStore := createTestRaftNode(t)

	fetcher := NewRaftDataFetcher(r, kvStore, logStore)

	// Set disconnected
	fetcher.SetConnected(false)
	if fetcher.IsConnected() {
		t.Error("Expected IsConnected to return false")
	}

	// Reconnect
	err := fetcher.Reconnect()
	if err != nil {
		t.Fatalf("Reconnect failed: %v", err)
	}

	// Should be connected now
	if !fetcher.IsConnected() {
		t.Error("Expected IsConnected to return true after Reconnect")
	}
}

// TestParseOperation tests the parseOperation helper function.
func TestParseOperation(t *testing.T) {
	testCases := []struct {
		name     string
		entry    *api.LogEntry
		expected string
	}{
		{
			name:     "nil entry",
			entry:    nil,
			expected: "unknown",
		},
		{
			name: "set command",
			entry: &api.LogEntry{
				Type: api.LogType_LOG_COMMAND,
				Data: []byte(`{"op":"set","key":"mykey","value":"myvalue"}`),
			},
			expected: "set mykey",
		},
		{
			name: "config entry",
			entry: &api.LogEntry{
				Type: api.LogType_LOG_CONFIGURATION,
				Data: []byte(`{}`),
			},
			expected: "config",
		},
		{
			name: "noop entry",
			entry: &api.LogEntry{
				Type: api.LogType_LOG_NOOP,
				Data: []byte{},
			},
			expected: "noop",
		},
		{
			name: "invalid json command",
			entry: &api.LogEntry{
				Type: api.LogType_LOG_COMMAND,
				Data: []byte(`not json`),
			},
			expected: "command",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := parseOperation(tc.entry)
			if result != tc.expected {
				t.Errorf("parseOperation() = %q, want %q", result, tc.expected)
			}
		})
	}
}
