// Package tui provides the terminal interface for monitoring and managing
// anchor's distributed key-value store cluster.
package tui

import (
	"fmt"
	"sync"
	"time"
)

// NodeHealth tracks health status for a node.
type NodeHealth struct {
	Connected      bool
	LastResponse   time.Time
	LastError      error
	ResponseTimeMs int64
}

// ElectionEvent represents a leader election event.
type ElectionEvent struct {
	Type      string // "started", "vote_requested", "leader_elected"
	NodeID    string
	Term      uint64
	Timestamp time.Time
}

// MultiNodeModel extends Model for multi-node cluster management.
type MultiNodeModel struct {
	*Model

	// Multi-node state
	NodeStates   map[string]*ClusterState // State per node
	NodeHealth   map[string]*NodeHealth   // Health per node
	ActiveNodeID string                   // Currently viewed node
	TotalNodes   int                      // Total nodes in cluster

	// Cluster-wide state
	ClusterLeaderID string
	ClusterTerm     uint64

	// Election state
	ElectionInProgress bool
	LastElectionEvent  *ElectionEvent
	ElectionEvents     []ElectionEvent

	// UI enhancements
	ColorSupport   bool
	UnicodeSupport bool

	mu sync.RWMutex
}

// NewMultiNodeModel creates a model for N nodes.
func NewMultiNodeModel(nodeCount int) *MultiNodeModel {
	m := &MultiNodeModel{
		Model:          NewModel(),
		NodeStates:     make(map[string]*ClusterState),
		NodeHealth:     make(map[string]*NodeHealth),
		TotalNodes:     nodeCount,
		ElectionEvents: make([]ElectionEvent, 0),
		ColorSupport:   true,
		UnicodeSupport: true,
	}

	// Initialize node IDs and health
	for i := 1; i <= nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		m.NodeHealth[nodeID] = &NodeHealth{
			Connected:    false,
			LastResponse: time.Time{},
		}
	}

	// Set first node as active by default
	if nodeCount > 0 {
		m.ActiveNodeID = "node1"
	}

	return m
}

// SetActiveNode switches the active node view.
// Returns true if the node exists and was switched, false otherwise.
func (m *MultiNodeModel) SetActiveNode(nodeID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if node exists (valid node ID format: node1, node2, ..., nodeN)
	if _, exists := m.NodeHealth[nodeID]; !exists {
		return false
	}

	m.ActiveNodeID = nodeID
	return true
}

// SetActiveNodeByNumber switches the active node view using a number (1-9).
// Returns true if the node exists and was switched, false otherwise.
func (m *MultiNodeModel) SetActiveNodeByNumber(num int) bool {
	if num < 1 || num > m.TotalNodes {
		return false
	}
	nodeID := fmt.Sprintf("node%d", num)
	return m.SetActiveNode(nodeID)
}

// GetActiveNodeState returns the state of the currently active node.
// Returns nil if no state is available for the active node.
func (m *MultiNodeModel) GetActiveNodeState() *ClusterState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.ActiveNodeID == "" {
		return nil
	}
	return m.NodeStates[m.ActiveNodeID]
}

// GetActiveNodeNumber returns the number of the currently active node (1-based).
// Returns 0 if no active node is set.
func (m *MultiNodeModel) GetActiveNodeNumber() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.ActiveNodeID == "" {
		return 0
	}

	var num int
	_, err := fmt.Sscanf(m.ActiveNodeID, "node%d", &num)
	if err != nil {
		return 0
	}
	return num
}

// UpdateNodeState updates the cluster state for a specific node.
func (m *MultiNodeModel) UpdateNodeState(nodeID string, state *ClusterState) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.NodeStates[nodeID] = state

	// Update cluster-wide state if this node reports leader info
	if state != nil && state.LeaderID != "" {
		m.ClusterLeaderID = state.LeaderID
		m.ClusterTerm = state.CurrentTerm
	}

	// Update the embedded Model's ClusterState if this is the active node
	if nodeID == m.ActiveNodeID {
		m.Model.ClusterState = state
	}
}

// UpdateNodeHealth updates health status for a node.
func (m *MultiNodeModel) UpdateNodeHealth(nodeID string, health *NodeHealth) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if health == nil {
		return
	}

	m.NodeHealth[nodeID] = health

	// Update connection state in embedded Model if this is the active node
	if nodeID == m.ActiveNodeID {
		m.Model.Connected = health.Connected
	}
}

// GetNodeHealth returns the health status for a specific node.
// Returns nil if the node doesn't exist.
func (m *MultiNodeModel) GetNodeHealth(nodeID string) *NodeHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.NodeHealth[nodeID]
}

// RecordElectionEvent records a leader election event.
func (m *MultiNodeModel) RecordElectionEvent(event *ElectionEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if event == nil {
		return
	}

	// Update election state
	switch event.Type {
	case "started":
		m.ElectionInProgress = true
	case "leader_elected":
		m.ElectionInProgress = false
		m.ClusterLeaderID = event.NodeID
		m.ClusterTerm = event.Term
	}

	m.LastElectionEvent = event

	// Add to election events log (keep last 10)
	if len(m.ElectionEvents) >= 10 {
		m.ElectionEvents = m.ElectionEvents[1:]
	}
	m.ElectionEvents = append(m.ElectionEvents, *event)

	// Add to logs panel
	m.Model.AddLogEntry(LogEntry{
		Index:     0,
		Term:      event.Term,
		Operation: fmt.Sprintf("election: %s (node: %s)", event.Type, event.NodeID),
		Timestamp: event.Timestamp,
	})
}

// GetAllNodeIDs returns a list of all node IDs in order.
func (m *MultiNodeModel) GetAllNodeIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodeIDs := make([]string, m.TotalNodes)
	for i := 1; i <= m.TotalNodes; i++ {
		nodeIDs[i-1] = fmt.Sprintf("node%d", i)
	}
	return nodeIDs
}

// IsNodeConnected returns whether a specific node is connected.
func (m *MultiNodeModel) IsNodeConnected(nodeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	health, exists := m.NodeHealth[nodeID]
	if !exists {
		return false
	}
	return health.Connected
}

// GetDisconnectedDuration returns how long a node has been disconnected.
// Returns 0 if the node is connected or doesn't exist.
func (m *MultiNodeModel) GetDisconnectedDuration(nodeID string) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	health, exists := m.NodeHealth[nodeID]
	if !exists || health.Connected {
		return 0
	}

	if health.LastResponse.IsZero() {
		return 0
	}

	return time.Since(health.LastResponse)
}

// GetLastElectionEvent returns the most recent election event.
// Returns nil if no election events have been recorded.
func (m *MultiNodeModel) GetLastElectionEvent() *ElectionEvent {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.LastElectionEvent
}

// GetElectionEvents returns a copy of all recorded election events.
// Returns an empty slice if no events have been recorded.
func (m *MultiNodeModel) GetElectionEvents() []ElectionEvent {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.ElectionEvents) == 0 {
		return []ElectionEvent{}
	}

	// Return a copy to avoid race conditions
	events := make([]ElectionEvent, len(m.ElectionEvents))
	copy(events, m.ElectionEvents)
	return events
}

// IsElectionInProgress returns whether an election is currently in progress.
func (m *MultiNodeModel) IsElectionInProgress() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.ElectionInProgress
}

// ClearElectionInProgress clears the election in progress flag.
// This is typically called after displaying the election notification.
func (m *MultiNodeModel) ClearElectionInProgress() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ElectionInProgress = false
}

// GetElectionEventCount returns the number of recorded election events.
func (m *MultiNodeModel) GetElectionEventCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.ElectionEvents)
}
