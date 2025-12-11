// Package tui provides integration tests for TUI startup and initialization.
package tui

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// integrationMockFetcher is a mock implementation of DataFetcher for integration testing.
type integrationMockFetcher struct {
	mu              sync.RWMutex
	connected       bool
	clusterState    *ClusterState
	logs            []LogEntry
	kvStore         map[string]string
	fetchError      error
	reconnectError  error
	reconnectCalled int
}

func newIntegrationMockFetcher() *integrationMockFetcher {
	return &integrationMockFetcher{
		connected: true,
		clusterState: &ClusterState{
			LocalNodeID: "node1",
			LocalRole:   "Leader",
			CurrentTerm: 5,
			CommitIndex: 100,
			LeaderID:    "node1",
			Nodes: []NodeStatus{
				{ID: "node1", Role: "Leader", Connected: true, MatchIndex: 100},
				{ID: "node2", Role: "Follower", Connected: true, MatchIndex: 98},
				{ID: "node3", Role: "Follower", Connected: true, MatchIndex: 95},
			},
			ReplicationLag: map[string]int64{
				"node2": 2,
				"node3": 5,
			},
			LastUpdated: time.Now(),
		},
		logs: []LogEntry{
			{Index: 98, Term: 5, Operation: "set key1", Timestamp: time.Now().Add(-2 * time.Second)},
			{Index: 99, Term: 5, Operation: "set key2", Timestamp: time.Now().Add(-1 * time.Second)},
			{Index: 100, Term: 5, Operation: "set key3", Timestamp: time.Now()},
		},
		kvStore: map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		},
	}
}

func (m *integrationMockFetcher) FetchClusterState() (*ClusterState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.fetchError != nil {
		return nil, m.fetchError
	}
	if !m.connected {
		return nil, errors.New("not connected")
	}
	return m.clusterState, nil
}

func (m *integrationMockFetcher) FetchRecentLogs(count int) ([]LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.connected {
		return nil, errors.New("not connected")
	}
	if count > len(m.logs) {
		return m.logs, nil
	}
	return m.logs[len(m.logs)-count:], nil
}

func (m *integrationMockFetcher) ExecuteGet(key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.connected {
		return "", errors.New("not connected")
	}
	if val, ok := m.kvStore[key]; ok {
		return val, nil
	}
	return "", errors.New("key not found")
}

func (m *integrationMockFetcher) ExecutePut(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.connected {
		return errors.New("not connected")
	}
	m.kvStore[key] = value
	return nil
}

func (m *integrationMockFetcher) IsConnected() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connected
}

func (m *integrationMockFetcher) Reconnect() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconnectCalled++
	if m.reconnectError != nil {
		return m.reconnectError
	}
	m.connected = true
	return nil
}

// TestTUIInitializesWithMockFetcher tests that TUI initializes correctly with a mock fetcher.
func TestTUIInitializesWithMockFetcher(t *testing.T) {
	fetcher := newIntegrationMockFetcher()
	app := NewApp(fetcher)

	if app == nil {
		t.Fatal("NewApp returned nil")
	}

	// Verify app components are initialized
	if app.model == nil {
		t.Error("Expected model to be initialized")
	}

	if app.view == nil {
		t.Error("Expected view to be initialized")
	}

	if app.fetcher == nil {
		t.Error("Expected fetcher to be set")
	}

	// Verify model has correct initial state
	model := app.GetModel()
	if model.ActivePanel != PanelStatus {
		t.Errorf("Expected initial panel to be Status, got %v", model.ActivePanel)
	}

	if model.RefreshInterval != time.Second {
		t.Errorf("Expected RefreshInterval to be 1s, got %v", model.RefreshInterval)
	}
}

// TestTUIDisplaysInitialClusterState tests that TUI displays initial cluster state.
func TestTUIDisplaysInitialClusterState(t *testing.T) {
	fetcher := newIntegrationMockFetcher()
	app := NewApp(fetcher)

	// Manually trigger refresh to populate cluster state
	app.refresh()

	model := app.GetModel()

	// Verify cluster state is populated
	if model.ClusterState == nil {
		t.Fatal("Expected ClusterState to be populated after refresh")
	}

	// Verify cluster state contains expected data
	state := model.ClusterState
	if state.LocalNodeID != "node1" {
		t.Errorf("Expected LocalNodeID to be 'node1', got %q", state.LocalNodeID)
	}

	if state.LocalRole != "Leader" {
		t.Errorf("Expected LocalRole to be 'Leader', got %q", state.LocalRole)
	}

	if state.CurrentTerm != 5 {
		t.Errorf("Expected CurrentTerm to be 5, got %d", state.CurrentTerm)
	}

	if state.CommitIndex != 100 {
		t.Errorf("Expected CommitIndex to be 100, got %d", state.CommitIndex)
	}

	if len(state.Nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(state.Nodes))
	}

	// Verify connection status
	if !model.Connected {
		t.Error("Expected Connected to be true")
	}
}

// TestTUIDisplaysNodeStatus tests that TUI correctly displays node status information.
func TestTUIDisplaysNodeStatus(t *testing.T) {
	fetcher := newIntegrationMockFetcher()
	app := NewApp(fetcher)

	// Trigger refresh
	app.refresh()

	model := app.GetModel()
	if model.ClusterState == nil {
		t.Fatal("Expected ClusterState to be populated")
	}

	// Verify each node's status
	nodes := model.ClusterState.Nodes
	expectedNodes := map[string]struct {
		role      string
		connected bool
	}{
		"node1": {"Leader", true},
		"node2": {"Follower", true},
		"node3": {"Follower", true},
	}

	for _, node := range nodes {
		expected, ok := expectedNodes[node.ID]
		if !ok {
			t.Errorf("Unexpected node ID: %s", node.ID)
			continue
		}

		if node.Role != expected.role {
			t.Errorf("Node %s: expected role %q, got %q", node.ID, expected.role, node.Role)
		}

		if node.Connected != expected.connected {
			t.Errorf("Node %s: expected connected=%v, got %v", node.ID, expected.connected, node.Connected)
		}
	}
}

// TestTUIDisplaysRecentLogs tests that TUI correctly fetches and displays recent logs.
func TestTUIDisplaysRecentLogs(t *testing.T) {
	fetcher := newIntegrationMockFetcher()
	app := NewApp(fetcher)

	// Trigger refresh
	app.refresh()

	model := app.GetModel()

	// Verify logs are populated
	if len(model.RecentLogs) == 0 {
		t.Fatal("Expected RecentLogs to be populated")
	}

	// Verify log entries contain expected data
	for _, log := range model.RecentLogs {
		if log.Index == 0 {
			t.Error("Expected log Index to be non-zero")
		}
		if log.Term == 0 {
			t.Error("Expected log Term to be non-zero")
		}
		if log.Operation == "" {
			t.Error("Expected log Operation to be non-empty")
		}
	}
}

// TestTUIRenderProducesOutput tests that TUI render produces non-empty output.
func TestTUIRenderProducesOutput(t *testing.T) {
	fetcher := newIntegrationMockFetcher()
	app := NewApp(fetcher)

	// Trigger refresh to populate state
	app.refresh()

	// Render the view
	output := app.view.Render(app.model)

	if output == "" {
		t.Error("Expected render to produce non-empty output")
	}

	// Verify output contains key information
	if !containsString(output, "node1") {
		t.Error("Expected output to contain node ID 'node1'")
	}

	if !containsString(output, "Leader") {
		t.Error("Expected output to contain role 'Leader'")
	}

	if !containsString(output, "Term") {
		t.Error("Expected output to contain 'Term'")
	}
}

// TestTUIModelViewSeparation tests that model and view are properly separated.
func TestTUIModelViewSeparation(t *testing.T) {
	fetcher := newIntegrationMockFetcher()
	app := NewApp(fetcher)

	// Get model and view
	model := app.GetModel()
	view := app.view

	// Verify model can be modified independently
	model.ActivePanel = PanelMetrics
	model.CommandInput = "test input"

	// Verify view can render the modified model
	output := view.Render(model)
	if output == "" {
		t.Error("Expected view to render modified model")
	}

	// Verify model changes don't affect view structure
	model.ActivePanel = PanelCommand
	output2 := view.Render(model)
	if output2 == "" {
		t.Error("Expected view to render model with different panel")
	}
}

// TestTUIFetcherInterface tests that the fetcher interface is properly mockable.
func TestTUIFetcherInterface(t *testing.T) {
	// Create mock fetcher
	fetcher := newIntegrationMockFetcher()

	// Verify all interface methods work
	state, err := fetcher.FetchClusterState()
	if err != nil {
		t.Errorf("FetchClusterState failed: %v", err)
	}
	if state == nil {
		t.Error("Expected non-nil cluster state")
	}

	logs, err := fetcher.FetchRecentLogs(10)
	if err != nil {
		t.Errorf("FetchRecentLogs failed: %v", err)
	}
	if logs == nil {
		t.Error("Expected non-nil logs")
	}

	value, err := fetcher.ExecuteGet("key1")
	if err != nil {
		t.Errorf("ExecuteGet failed: %v", err)
	}
	if value != "value1" {
		t.Errorf("Expected value 'value1', got %q", value)
	}

	err = fetcher.ExecutePut("newkey", "newvalue")
	if err != nil {
		t.Errorf("ExecutePut failed: %v", err)
	}

	if !fetcher.IsConnected() {
		t.Error("Expected IsConnected to return true")
	}

	err = fetcher.Reconnect()
	if err != nil {
		t.Errorf("Reconnect failed: %v", err)
	}
}

// containsString checks if s contains substr.
func containsString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
