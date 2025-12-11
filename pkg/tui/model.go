package tui

import (
	"time"
)

// PanelType identifies which panel has focus.
type PanelType int

const (
	PanelStatus PanelType = iota
	PanelMetrics
	PanelReplication
	PanelLogs
	PanelCommand
)

// String returns a human-readable representation of the PanelType.
func (p PanelType) String() string {
	switch p {
	case PanelStatus:
		return "Status"
	case PanelMetrics:
		return "Metrics"
	case PanelReplication:
		return "Replication"
	case PanelLogs:
		return "Logs"
	case PanelCommand:
		return "Command"
	default:
		return "Unknown"
	}
}

// PanelCount is the total number of panels for navigation.
const PanelCount = 5

// Model holds the application state for the TUI.
type Model struct {
	// Cluster state
	ClusterState *ClusterState
	RecentLogs   []LogEntry

	// UI state
	ActivePanel   PanelType
	CommandInput  string
	CommandOutput string
	ErrorMessage  string

	// Connection state
	Connected         bool
	ReconnectAttempts int
	LastReconnect     time.Time

	// Configuration
	RefreshInterval time.Duration
}

// NewModel creates a new Model with default values.
func NewModel() *Model {
	return &Model{
		ActivePanel:     PanelStatus,
		Connected:       true,
		RefreshInterval: time.Second,
		RecentLogs:      make([]LogEntry, 0, 10),
	}
}

// NextPanel moves focus to the next panel in circular order.
// Order: Status → Metrics → Replication → Logs → Command → Status
func (m *Model) NextPanel() {
	m.ActivePanel = PanelType((int(m.ActivePanel) + 1) % PanelCount)
}

// PrevPanel moves focus to the previous panel in circular order.
// Order: Status → Command → Logs → Replication → Metrics → Status
func (m *Model) PrevPanel() {
	m.ActivePanel = PanelType((int(m.ActivePanel) - 1 + PanelCount) % PanelCount)
}

// AddLogEntry adds a new log entry to the recent logs buffer.
// Maintains a maximum of 10 entries, removing the oldest when full.
func (m *Model) AddLogEntry(entry LogEntry) {
	if len(m.RecentLogs) >= 10 {
		// Remove oldest entry (first element)
		m.RecentLogs = m.RecentLogs[1:]
	}
	m.RecentLogs = append(m.RecentLogs, entry)
}
