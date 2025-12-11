// Package tui provides panel renderers for the TUI dashboard.
package tui

import (
	"fmt"
	"strings"
	"time"
)

// StatusPanel renders the node status information.
type StatusPanel struct{}

// NewStatusPanel creates a new StatusPanel.
func NewStatusPanel() *StatusPanel {
	return &StatusPanel{}
}

// Render outputs the node status panel content.
// Displays each node's ID, role, and connection status.
// Disconnected nodes are marked appropriately.
func (p *StatusPanel) Render(state *ClusterState) string {
	if state == nil {
		return "No cluster state available"
	}

	var sb strings.Builder
	sb.WriteString("=== Node Status ===\n")
	sb.WriteString(fmt.Sprintf("Local Node: %s (%s)\n", state.LocalNodeID, state.LocalRole))
	sb.WriteString(fmt.Sprintf("Leader: %s\n", state.LeaderID))
	sb.WriteString("\nCluster Nodes:\n")

	for _, node := range state.Nodes {
		status := "connected"
		if !node.Connected {
			status = "disconnected"
		}
		sb.WriteString(fmt.Sprintf("  %s: %s [%s]\n", node.ID, node.Role, status))
	}

	return sb.String()
}

// MetricsPanel renders the Raft metrics information.
type MetricsPanel struct{}

// NewMetricsPanel creates a new MetricsPanel.
func NewMetricsPanel() *MetricsPanel {
	return &MetricsPanel{}
}

// Render outputs the metrics panel content.
// Displays term, commit index, and last updated timestamp.
func (p *MetricsPanel) Render(state *ClusterState) string {
	if state == nil {
		return "No cluster state available"
	}

	var sb strings.Builder
	sb.WriteString("=== Metrics ===\n")
	sb.WriteString(fmt.Sprintf("Current Term: %d\n", state.CurrentTerm))
	sb.WriteString(fmt.Sprintf("Commit Index: %d\n", state.CommitIndex))
	sb.WriteString(fmt.Sprintf("Last Updated: %s\n", state.LastUpdated.Format(time.RFC3339)))

	return sb.String()
}

// ReplicationPanel renders the log replication progress.
type ReplicationPanel struct{}

// NewReplicationPanel creates a new ReplicationPanel.
func NewReplicationPanel() *ReplicationPanel {
	return &ReplicationPanel{}
}

// Render outputs the replication panel content.
// For leaders: displays each follower's match index, highlighting lagging followers (>100 entries behind).
// For non-leaders: displays a message indicating replication data is only available on the leader.
func (p *ReplicationPanel) Render(state *ClusterState) string {
	if state == nil {
		return "No cluster state available"
	}

	var sb strings.Builder
	sb.WriteString("=== Replication ===\n")

	if state.LocalRole != "Leader" {
		sb.WriteString("Replication data is only available on the leader\n")
		return sb.String()
	}

	sb.WriteString("Follower Match Indices:\n")
	for _, node := range state.Nodes {
		if node.ID == state.LocalNodeID {
			continue // Skip self
		}

		lag, hasLag := state.ReplicationLag[node.ID]
		lagIndicator := ""
		if hasLag && lag > 100 {
			lagIndicator = " [LAGGING]"
		}

		sb.WriteString(fmt.Sprintf("  %s: match=%d%s\n", node.ID, node.MatchIndex, lagIndicator))
	}

	return sb.String()
}

// LogsPanel renders the recent log entries.
type LogsPanel struct{}

// NewLogsPanel creates a new LogsPanel.
func NewLogsPanel() *LogsPanel {
	return &LogsPanel{}
}

// Render outputs the logs panel content.
// Displays up to 10 recent log entries with index, term, and operation.
func (p *LogsPanel) Render(logs []LogEntry) string {
	var sb strings.Builder
	sb.WriteString("=== Recent Logs ===\n")

	if len(logs) == 0 {
		sb.WriteString("No log entries\n")
		return sb.String()
	}

	// Display at most 10 entries
	displayCount := len(logs)
	if displayCount > 10 {
		displayCount = 10
	}

	// Show the most recent entries (last N entries)
	startIdx := len(logs) - displayCount
	for i := startIdx; i < len(logs); i++ {
		entry := logs[i]
		sb.WriteString(fmt.Sprintf("  [%d] term=%d op=%s\n", entry.Index, entry.Term, entry.Operation))
	}

	return sb.String()
}

// CommandPanel renders the command input and output area.
type CommandPanel struct{}

// NewCommandPanel creates a new CommandPanel.
func NewCommandPanel() *CommandPanel {
	return &CommandPanel{}
}

// Render outputs the command panel content.
// Displays the current input, output, and any error messages.
func (p *CommandPanel) Render(input, output, errorMsg string) string {
	var sb strings.Builder
	sb.WriteString("=== Command ===\n")
	sb.WriteString(fmt.Sprintf("> %s\n", input))

	if errorMsg != "" {
		sb.WriteString(fmt.Sprintf("Error: %s\n", errorMsg))
	}

	if output != "" {
		sb.WriteString(fmt.Sprintf("Output: %s\n", output))
	}

	return sb.String()
}

// RenderNotLeaderError formats an error message for not-leader errors.
// Includes the leader ID to help the user redirect their request.
func (p *CommandPanel) RenderNotLeaderError(leaderID string) string {
	return fmt.Sprintf("Not leader. Current leader is: %s", leaderID)
}
