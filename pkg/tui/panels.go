// Package tui provides panel renderers for the TUI dashboard.
package tui

import (
	"fmt"
	"strings"
)

// StatusPanel shows node connectivity and roles.
type StatusPanel struct{}

func NewStatusPanel() *StatusPanel { return &StatusPanel{} }

func (p *StatusPanel) Draw(b *Buffer, x, y, w, h int, state *ClusterState, isActive bool) {
	// Draw border
	style := CurrentStyles.Border
	if isActive {
		style = CurrentStyles.BorderFocus
	}
	b.DrawBox(x, y, w, h, style)
	b.DrawString(x+2, y, " Nodes ", CurrentStyles.Header)

	if state == nil {
		b.DrawString(x+2, y+2, "No state", CurrentStyles.Muted)
		return
	}

	// Content area
	contentX, contentY := x+2, y+2

	// Local node info
	b.DrawString(contentX, contentY, fmt.Sprintf("Local: %s", state.LocalNodeID), CurrentStyles.Bold)
	roleStyle := CurrentStyles.Normal
	if state.LocalRole == "Leader" {
		roleStyle = CurrentStyles.Success
	} else if state.LocalRole == "Candidate" {
		roleStyle = CurrentStyles.Warning
	}
	b.DrawString(contentX, contentY+1, state.LocalRole, roleStyle)

	b.DrawString(contentX, contentY+3, "Cluster Members:", CurrentStyles.Muted)

	// List nodes
	currentY := contentY + 5
	for _, node := range state.Nodes {
		if currentY >= y+h-1 {
			break
		}

		statusStyle := CurrentStyles.Success
		statusChar := '●'
		if !node.Connected {
			statusStyle = CurrentStyles.Error
			statusChar = '○'
		}

		b.Set(contentX, currentY, statusChar, statusStyle)

		nodeStyle := CurrentStyles.Normal
		if node.ID == state.LeaderID {
			nodeStyle = CurrentStyles.Warning // Gold for leader
		}

		nodeStr := fmt.Sprintf(" %s (%s)", node.ID, node.Role)
		b.DrawString(contentX+1, currentY, nodeStr, nodeStyle)

		currentY++
	}
}

// MetricsPanel displays current term and commit index.
type MetricsPanel struct{}

func NewMetricsPanel() *MetricsPanel { return &MetricsPanel{} }

func (p *MetricsPanel) Draw(b *Buffer, x, y, w, h int, state *ClusterState, isActive bool) {
	style := CurrentStyles.Border
	if isActive {
		style = CurrentStyles.BorderFocus
	}
	b.DrawBox(x, y, w, h, style)
	b.DrawString(x+2, y, " Metrics ", CurrentStyles.Header)

	if state == nil {
		return
	}

	// Simple key-value display
	drawKV := func(key, value string, row int) {
		b.DrawString(x+2, y+2+row, key, CurrentStyles.Muted)
		b.DrawString(x+2+15, y+2+row, value, CurrentStyles.Normal)
	}

	drawKV("Term", fmt.Sprintf("%d", state.CurrentTerm), 0)
	drawKV("Commit Index", fmt.Sprintf("%d", state.CommitIndex), 1)
	drawKV("Last Updates", state.LastUpdated.Format("15:04:05"), 2)
}

// ReplicationPanel tracks follower synchronization status.
type ReplicationPanel struct{}

func NewReplicationPanel() *ReplicationPanel { return &ReplicationPanel{} }

func (p *ReplicationPanel) Draw(b *Buffer, x, y, w, h int, state *ClusterState, isActive bool) {
	style := CurrentStyles.Border
	if isActive {
		style = CurrentStyles.BorderFocus
	}
	b.DrawBox(x, y, w, h, style)
	b.DrawString(x+2, y, " Replication ", CurrentStyles.Header)

	if state == nil {
		return
	}

	if state.LocalRole != "Leader" {
		b.DrawString(x+2, y+2, "Replication view only available on Leader", CurrentStyles.Muted)
		return
	}

	row := 0
	for _, node := range state.Nodes {
		if node.ID == state.LocalNodeID {
			continue
		}

		if y+2+row*3 >= y+h-1 {
			break
		}

		// Node info
		b.DrawString(x+2, y+2+row*3, node.ID, CurrentStyles.Bold)
		b.DrawString(x+15, y+2+row*3, fmt.Sprintf("Match: %d", node.MatchIndex), CurrentStyles.Muted)

		// Visual representation of replication lag.
		// CommitIndex is the target.
		pct := 1.0
		if state.CommitIndex > 0 {
			pct = float64(node.MatchIndex) / float64(state.CommitIndex)
		}
		if pct > 1.0 {
			pct = 1.0
		}

		barW := w - 4
		b.DrawBar(x+2, y+2+row*3+1, barW, pct, CurrentStyles.Selected, CurrentStyles.Muted)

		row++
	}
}

// LogsPanel lists the latest Raft operations.
type LogsPanel struct{}

func NewLogsPanel() *LogsPanel { return &LogsPanel{} }

func (p *LogsPanel) Draw(b *Buffer, x, y, w, h int, logs []LogEntry, isActive bool) {
	style := CurrentStyles.Border
	if isActive {
		style = CurrentStyles.BorderFocus
	}
	b.DrawBox(x, y, w, h, style)
	b.DrawString(x+2, y, " Logs ", CurrentStyles.Header)

	if len(logs) == 0 {
		b.DrawString(x+2, y+2, "No logs", CurrentStyles.Muted)
		return
	}

	// Display logs
	// Cap at available height
	maxLines := h - 2
	count := len(logs)
	if count > maxLines {
		count = maxLines
	}

	startIdx := len(logs) - count
	for i := 0; i < count; i++ {
		entry := logs[startIdx+i]
		rowY := y + 2 + i

		// Draw Index
		idxStr := fmt.Sprintf("[%d]", entry.Index)
		b.DrawString(x+2, rowY, idxStr, CurrentStyles.Muted)

		// Draw details
		logStr := fmt.Sprintf(" Term: %d | %s", entry.Term, entry.Operation)
		b.DrawString(x+2+len(idxStr)+1, rowY, logStr, CurrentStyles.Normal)
	}
}

// CommandPanel processes user CLI input.
type CommandPanel struct{}

func NewCommandPanel() *CommandPanel { return &CommandPanel{} }

func (p *CommandPanel) Draw(b *Buffer, x, y, w, h int, input, output, errorMsg string, isActive bool) {
	style := CurrentStyles.Border
	if isActive {
		style = CurrentStyles.BorderFocus
	}
	b.DrawBox(x, y, w, h, style)
	b.DrawString(x+2, y, " Command ", CurrentStyles.Header)

	// Input area
	prompt := "> "
	b.DrawString(x+2, y+2, prompt, CurrentStyles.Bold)
	b.DrawString(x+2+len(prompt), y+2, input, CurrentStyles.Normal)

	// Cursor (fake) if active
	if isActive {
		cursorX := x + 2 + len(prompt) + len(input)
		if cursorX < x+w-1 {
			b.Set(cursorX, y+2, '█', CurrentStyles.Highlight)
		}
	}

	// Divider
	b.DrawString(x+2, y+4, strings.Repeat("─", w-4), CurrentStyles.Muted)

	// Output Area
	if errorMsg != "" {
		b.DrawString(x+2, y+5, "Error: "+errorMsg, CurrentStyles.Error)
	} else if output != "" {
		b.DrawString(x+2, y+5, output, CurrentStyles.Success)
	}
}

// RenderNotLeaderError helps format redirect messages when hitting a follower.
func (p *CommandPanel) RenderNotLeaderError(leaderID string) string {
	return fmt.Sprintf("Not leader. Current leader is: %s", leaderID)
}
