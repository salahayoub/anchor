// Package tui provides unit tests for the App controller.
package tui

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gdamore/tcell/v2"
)

// mockDataFetcher is a mock implementation of DataFetcher for testing.
type mockDataFetcher struct {
	mu              sync.RWMutex
	connected       bool
	clusterState    *ClusterState
	logs            []LogEntry
	kvStore         map[string]string
	fetchError      error
	reconnectError  error
	reconnectCalled int
}

func newMockDataFetcher() *mockDataFetcher {
	return &mockDataFetcher{
		connected: true,
		clusterState: &ClusterState{
			LocalNodeID: "node1",
			LocalRole:   "Follower",
			CurrentTerm: 1,
			CommitIndex: 10,
			LeaderID:    "node2",
			Nodes: []NodeStatus{
				{ID: "node1", Role: "Follower", Connected: true, MatchIndex: 10},
				{ID: "node2", Role: "Leader", Connected: true, MatchIndex: 10},
			},
			LastUpdated: time.Now(),
		},
		logs:    []LogEntry{},
		kvStore: make(map[string]string),
	}
}

func (m *mockDataFetcher) FetchClusterState() (*ClusterState, error) {
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

func (m *mockDataFetcher) FetchRecentLogs(count int) ([]LogEntry, error) {
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

func (m *mockDataFetcher) ExecuteGet(key string) (string, error) {
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

func (m *mockDataFetcher) ExecutePut(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.connected {
		return errors.New("not connected")
	}
	// Simulate not-leader error for followers
	if m.clusterState != nil && m.clusterState.LocalRole != "Leader" {
		return errors.New("not leader, leader is: " + m.clusterState.LeaderID)
	}
	m.kvStore[key] = value
	return nil
}

func (m *mockDataFetcher) IsConnected() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connected
}

func (m *mockDataFetcher) Reconnect() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconnectCalled++
	if m.reconnectError != nil {
		return m.reconnectError
	}
	m.connected = true
	return nil
}

func (m *mockDataFetcher) SetConnected(connected bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connected = connected
}

func (m *mockDataFetcher) SetFetchError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fetchError = err
}

func (m *mockDataFetcher) SetReconnectError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconnectError = err
}

func (m *mockDataFetcher) GetReconnectCalled() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.reconnectCalled
}

// TestNewApp tests that NewApp creates an App with correct defaults.
func TestNewApp(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	if app == nil {
		t.Fatal("NewApp returned nil")
	}

	if app.model == nil {
		t.Error("Expected model to be initialized")
	}

	if app.view == nil {
		t.Error("Expected view to be initialized")
	}

	if app.fetcher != fetcher {
		t.Error("Expected fetcher to be set")
	}

	if app.reconnectInterval != 5*time.Second {
		t.Errorf("Expected reconnectInterval to be 5s, got %v", app.reconnectInterval)
	}

	if app.reconnectTimeout != 30*time.Second {
		t.Errorf("Expected reconnectTimeout to be 30s, got %v", app.reconnectTimeout)
	}
}

// TestApp_HandleKeyEvent_Tab tests Tab key navigation.
func TestApp_HandleKeyEvent_Tab(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	// Initial panel should be Status
	if app.model.ActivePanel != PanelStatus {
		t.Errorf("Expected initial panel to be Status, got %v", app.model.ActivePanel)
	}

	// Press Tab - should move to Metrics
	event := KeyEvent{Key: tcell.KeyTab}
	shouldExit := app.handleKeyEvent(event)

	if shouldExit {
		t.Error("Tab should not cause exit")
	}

	if app.model.ActivePanel != PanelMetrics {
		t.Errorf("Expected panel to be Metrics after Tab, got %v", app.model.ActivePanel)
	}

	// Reset debounce state for second key press
	app.lastKeyTime = time.Time{}

	// Press Tab again - should move to Replication
	app.handleKeyEvent(event)
	if app.model.ActivePanel != PanelReplication {
		t.Errorf("Expected panel to be Replication after second Tab, got %v", app.model.ActivePanel)
	}
}

// TestApp_HandleKeyEvent_ShiftTab tests Shift+Tab key navigation.
func TestApp_HandleKeyEvent_ShiftTab(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	// Initial panel should be Status
	if app.model.ActivePanel != PanelStatus {
		t.Errorf("Expected initial panel to be Status, got %v", app.model.ActivePanel)
	}

	// Press Shift+Tab - should move to Command (circular)
	event := KeyEvent{Key: tcell.KeyBacktab}
	shouldExit := app.handleKeyEvent(event)

	if shouldExit {
		t.Error("Shift+Tab should not cause exit")
	}

	if app.model.ActivePanel != PanelCommand {
		t.Errorf("Expected panel to be Command after Shift+Tab, got %v", app.model.ActivePanel)
	}

	// Reset debounce state for second key press
	app.lastKeyTime = time.Time{}

	// Press Shift+Tab again - should move to Logs
	app.handleKeyEvent(event)
	if app.model.ActivePanel != PanelLogs {
		t.Errorf("Expected panel to be Logs after second Shift+Tab, got %v", app.model.ActivePanel)
	}
}

// TestApp_HandleKeyEvent_Quit tests 'q' key for exit.
func TestApp_HandleKeyEvent_Quit(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	// Press 'q' - should exit
	event := KeyEvent{Key: tcell.KeyRune, Rune: 'q'}
	shouldExit := app.handleKeyEvent(event)

	if !shouldExit {
		t.Error("'q' should cause exit")
	}
}

// TestApp_HandleKeyEvent_CtrlC tests Ctrl+C for exit.
func TestApp_HandleKeyEvent_CtrlC(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	// Press Ctrl+C - should exit
	event := KeyEvent{Key: tcell.KeyCtrlC}
	shouldExit := app.handleKeyEvent(event)

	if !shouldExit {
		t.Error("Ctrl+C should cause exit")
	}
}

// TestApp_HandleKeyEvent_Refresh tests 'r' key for refresh.
func TestApp_HandleKeyEvent_Refresh(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	// Clear cluster state to verify refresh works
	app.model.ClusterState = nil

	// Press 'r' - should refresh
	event := KeyEvent{Key: tcell.KeyRune, Rune: 'r'}
	shouldExit := app.handleKeyEvent(event)

	if shouldExit {
		t.Error("'r' should not cause exit")
	}

	// Cluster state should be populated after refresh
	if app.model.ClusterState == nil {
		t.Error("Expected ClusterState to be populated after refresh")
	}
}

// TestApp_HandleKeyEvent_CommandInput tests character input in command panel.
func TestApp_HandleKeyEvent_CommandInput(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	// Switch to command panel
	app.model.ActivePanel = PanelCommand

	// Type "GET"
	app.handleKeyEvent(KeyEvent{Key: tcell.KeyRune, Rune: 'G'})
	app.handleKeyEvent(KeyEvent{Key: tcell.KeyRune, Rune: 'E'})
	app.handleKeyEvent(KeyEvent{Key: tcell.KeyRune, Rune: 'T'})

	if app.model.CommandInput != "GET" {
		t.Errorf("Expected CommandInput to be 'GET', got %q", app.model.CommandInput)
	}
}

// TestApp_HandleKeyEvent_CommandBackspace tests backspace in command panel.
func TestApp_HandleKeyEvent_CommandBackspace(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	// Switch to command panel and set input
	app.model.ActivePanel = PanelCommand
	app.model.CommandInput = "GET"

	// Press backspace
	app.handleKeyEvent(KeyEvent{Key: tcell.KeyBackspace2})

	if app.model.CommandInput != "GE" {
		t.Errorf("Expected CommandInput to be 'GE' after backspace, got %q", app.model.CommandInput)
	}
}

// TestApp_HandleKeyEvent_CommandEscape tests escape to clear input.
func TestApp_HandleKeyEvent_CommandEscape(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	// Switch to command panel and set input
	app.model.ActivePanel = PanelCommand
	app.model.CommandInput = "GET key"
	app.model.ErrorMessage = "some error"

	// Press escape
	app.handleKeyEvent(KeyEvent{Key: tcell.KeyEscape})

	if app.model.CommandInput != "" {
		t.Errorf("Expected CommandInput to be empty after escape, got %q", app.model.CommandInput)
	}

	if app.model.ErrorMessage != "" {
		t.Errorf("Expected ErrorMessage to be empty after escape, got %q", app.model.ErrorMessage)
	}
}

// TestApp_HandleKeyEvent_CommandEnter_Get tests Enter to execute GET command.
func TestApp_HandleKeyEvent_CommandEnter_Get(t *testing.T) {
	fetcher := newMockDataFetcher()
	fetcher.kvStore["mykey"] = "myvalue"
	app := NewApp(fetcher)

	// Switch to command panel and set input
	app.model.ActivePanel = PanelCommand
	app.model.CommandInput = "GET mykey"

	// Press Enter
	app.handleKeyEvent(KeyEvent{Key: tcell.KeyEnter})

	if app.model.CommandOutput != "myvalue" {
		t.Errorf("Expected CommandOutput to be 'myvalue', got %q", app.model.CommandOutput)
	}

	if app.model.CommandInput != "" {
		t.Errorf("Expected CommandInput to be cleared after execution, got %q", app.model.CommandInput)
	}
}

// TestApp_HandleKeyEvent_CommandEnter_Put_NotLeader tests PUT command when not leader.
func TestApp_HandleKeyEvent_CommandEnter_Put_NotLeader(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	// Switch to command panel and set input
	app.model.ActivePanel = PanelCommand
	app.model.CommandInput = "PUT mykey myvalue"

	// Press Enter - should fail because we're not leader
	app.handleKeyEvent(KeyEvent{Key: tcell.KeyEnter})

	if app.model.ErrorMessage == "" {
		t.Error("Expected ErrorMessage to be set for not-leader error")
	}

	// Error should mention the leader
	if !contains(app.model.ErrorMessage, "leader") {
		t.Errorf("Expected error to mention leader, got %q", app.model.ErrorMessage)
	}
}

// TestApp_HandleKeyEvent_CommandEnter_InvalidCommand tests Enter with invalid command.
func TestApp_HandleKeyEvent_CommandEnter_InvalidCommand(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	// Switch to command panel and set invalid input
	app.model.ActivePanel = PanelCommand
	app.model.CommandInput = "INVALID command"

	// Press Enter
	app.handleKeyEvent(KeyEvent{Key: tcell.KeyEnter})

	if app.model.ErrorMessage == "" {
		t.Error("Expected ErrorMessage to be set for invalid command")
	}
}

// TestApp_HandleKeyEvent_QuitInCommandPanel tests 'q' doesn't quit when typing in command panel.
func TestApp_HandleKeyEvent_QuitInCommandPanel(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	// Switch to command panel and set some input
	app.model.ActivePanel = PanelCommand
	app.model.CommandInput = "GET ke"

	// Press 'q' - should add to input, not quit
	event := KeyEvent{Key: tcell.KeyRune, Rune: 'q'}
	shouldExit := app.handleKeyEvent(event)

	if shouldExit {
		t.Error("'q' should not cause exit when command input is not empty")
	}

	if app.model.CommandInput != "GET keq" {
		t.Errorf("Expected 'q' to be added to input, got %q", app.model.CommandInput)
	}
}

// TestApp_HandleKeyEvent_QuitInCommandPanelEmpty tests 'q' quits when command input is empty.
func TestApp_HandleKeyEvent_QuitInCommandPanelEmpty(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	// Switch to command panel with empty input
	app.model.ActivePanel = PanelCommand
	app.model.CommandInput = ""

	// Press 'q' - should quit
	event := KeyEvent{Key: tcell.KeyRune, Rune: 'q'}
	shouldExit := app.handleKeyEvent(event)

	if !shouldExit {
		t.Error("'q' should cause exit when command input is empty")
	}
}

// TestApp_Stop tests graceful shutdown.
func TestApp_Stop(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	// Mark as running
	app.mu.Lock()
	app.running = true
	app.mu.Unlock()

	// Stop should not panic
	app.Stop()

	if app.IsRunning() {
		t.Error("Expected IsRunning to return false after Stop")
	}

	// Calling Stop again should not panic
	app.Stop()
}

// TestApp_AttemptReconnect_Success tests successful reconnection.
func TestApp_AttemptReconnect_Success(t *testing.T) {
	fetcher := newMockDataFetcher()
	fetcher.SetConnected(false)
	app := NewApp(fetcher)
	app.model.Connected = false
	app.model.ReconnectAttempts = 2

	app.attemptReconnect()

	if !app.model.Connected {
		t.Error("Expected Connected to be true after successful reconnect")
	}

	if app.model.ReconnectAttempts != 0 {
		t.Errorf("Expected ReconnectAttempts to be reset to 0, got %d", app.model.ReconnectAttempts)
	}

	if app.model.ErrorMessage != "" {
		t.Errorf("Expected ErrorMessage to be cleared, got %q", app.model.ErrorMessage)
	}
}

// TestApp_AttemptReconnect_Failure tests failed reconnection.
func TestApp_AttemptReconnect_Failure(t *testing.T) {
	fetcher := newMockDataFetcher()
	fetcher.SetConnected(false)
	fetcher.SetReconnectError(errors.New("connection refused"))
	app := NewApp(fetcher)
	app.model.Connected = false
	app.model.ReconnectAttempts = 0

	app.attemptReconnect()

	if app.model.Connected {
		t.Error("Expected Connected to remain false after failed reconnect")
	}

	if app.model.ReconnectAttempts != 1 {
		t.Errorf("Expected ReconnectAttempts to be 1, got %d", app.model.ReconnectAttempts)
	}

	if app.model.ErrorMessage == "" {
		t.Error("Expected ErrorMessage to be set after failed reconnect")
	}
}

// TestApp_AttemptReconnect_Timeout tests reconnection timeout after 30 seconds.
func TestApp_AttemptReconnect_Timeout(t *testing.T) {
	fetcher := newMockDataFetcher()
	fetcher.SetConnected(false)
	fetcher.SetReconnectError(errors.New("connection refused"))
	app := NewApp(fetcher)
	app.model.Connected = false
	// Set attempts to exceed timeout (30s / 5s = 6 attempts)
	app.model.ReconnectAttempts = 7

	app.attemptReconnect()

	// Should show troubleshooting guidance
	if !contains(app.model.ErrorMessage, "30 seconds") {
		t.Errorf("Expected error to mention 30 seconds timeout, got %q", app.model.ErrorMessage)
	}
}

// TestApp_Refresh tests the refresh function.
func TestApp_Refresh(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	// Clear state
	app.model.ClusterState = nil
	app.model.RecentLogs = nil

	app.refresh()

	if app.model.ClusterState == nil {
		t.Error("Expected ClusterState to be populated after refresh")
	}

	if !app.model.Connected {
		t.Error("Expected Connected to be true after successful refresh")
	}
}

// TestApp_Refresh_Disconnected tests refresh when disconnected.
func TestApp_Refresh_Disconnected(t *testing.T) {
	fetcher := newMockDataFetcher()
	fetcher.SetConnected(false)
	app := NewApp(fetcher)
	app.model.Connected = true

	app.refresh()

	if app.model.Connected {
		t.Error("Expected Connected to be false when fetcher is disconnected")
	}
}

// TestApp_GetModel tests the GetModel accessor.
func TestApp_GetModel(t *testing.T) {
	fetcher := newMockDataFetcher()
	app := NewApp(fetcher)

	model := app.GetModel()

	if model == nil {
		t.Error("Expected GetModel to return non-nil model")
	}

	if model != app.model {
		t.Error("Expected GetModel to return the same model instance")
	}
}

// TestIsNotLeaderError tests the isNotLeaderError helper.
func TestIsNotLeaderError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"not leader error", errors.New("not leader"), true},
		{"not the leader error", errors.New("not the leader"), true},
		{"other error", errors.New("connection refused"), false},
		{"case insensitive", errors.New("NOT LEADER"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isNotLeaderError(tt.err)
			if result != tt.expected {
				t.Errorf("isNotLeaderError(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

// TestExtractLeaderID tests the extractLeaderID helper.
func TestExtractLeaderID(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{"nil error", nil, "unknown"},
		{"leader is format", errors.New("not leader, leader is: node2"), "node2"},
		{"leader format", errors.New("not leader, leader: node3"), "node3"},
		{"no leader info", errors.New("not leader"), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractLeaderID(tt.err)
			if result != tt.expected {
				t.Errorf("extractLeaderID(%v) = %q, want %q", tt.err, result, tt.expected)
			}
		})
	}
}
