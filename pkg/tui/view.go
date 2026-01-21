package tui

import (
	"fmt"
	"strings"
)

// View manages the TUI layout and rendering.
type View struct {
	statusPanel      *StatusPanel
	metricsPanel     *MetricsPanel
	replicationPanel *ReplicationPanel
	logsPanel        *LogsPanel
	commandPanel     *CommandPanel
}

// NewView creates a new View with all panel renderers initialized.
func NewView() *View {
	return &View{
		statusPanel:      NewStatusPanel(),
		metricsPanel:     NewMetricsPanel(),
		replicationPanel: NewReplicationPanel(),
		logsPanel:        NewLogsPanel(),
		commandPanel:     NewCommandPanel(),
	}
}

// Render draws the entire application to the buffer.
func (v *View) Render(b *Buffer, model *Model) {
	width, height := b.Width, b.Height

	// Draw Background
	b.FillRect(0, 0, width, height, ' ', CurrentStyles.Normal)

	// Layout Constants
	headerHeight := 3
	footerHeight := 3
	mainHeight := height - headerHeight - footerHeight

	if mainHeight < 0 {
		mainHeight = 0
	}

	// Header
	v.drawHeader(b, 0, 0, width, headerHeight, model)

	// Main Content Area
	// Sidebar / Content split
	sidebarWidth := width / 4
	if sidebarWidth < 25 {
		sidebarWidth = 25
	}
	if sidebarWidth > width {
		sidebarWidth = width
	}
	contentWidth := width - sidebarWidth

	mainY := headerHeight

	// Sidebar: Status Panel typically
	v.statusPanel.Draw(b, 0, mainY, sidebarWidth, mainHeight, model.ClusterState, model.ActivePanel == PanelStatus)

	// Content Area
	contentX := sidebarWidth

	topRowHeight := mainHeight / 3
	midRowHeight := mainHeight - topRowHeight - 4 // 4 for command panel
	commandHeight := 4

	if topRowHeight < 5 {
		topRowHeight = 5
	}
	if commandHeight < 3 {
		commandHeight = 3
	}

	// Re-adjust if height is small
	remaining := mainHeight - commandHeight
	if remaining < 10 {
		topRowHeight = remaining / 2
		midRowHeight = remaining - topRowHeight
	}

	// Metrics & Replication
	metricsWidth := contentWidth / 2
	replWidth := contentWidth - metricsWidth

	v.metricsPanel.Draw(b, contentX, mainY, metricsWidth, topRowHeight, model.ClusterState, model.ActivePanel == PanelMetrics)
	v.replicationPanel.Draw(b, contentX+metricsWidth, mainY, replWidth, topRowHeight, model.ClusterState, model.ActivePanel == PanelReplication)

	// Logs
	v.logsPanel.Draw(b, contentX, mainY+topRowHeight, contentWidth, midRowHeight, model.RecentLogs, model.ActivePanel == PanelLogs)

	// Command
	v.commandPanel.Draw(b, contentX, mainY+topRowHeight+midRowHeight, contentWidth, commandHeight, model.CommandInput, model.CommandOutput, model.ErrorMessage, model.ActivePanel == PanelCommand)

	// Footer
	v.drawFooter(b, 0, height-footerHeight, width, footerHeight, model)
}

func (v *View) drawHeader(b *Buffer, x, y, w, h int, model *Model) {
	b.FillRect(x, y, w, h, ' ', CurrentStyles.Normal)

	title := " ANCHOR "
	b.DrawString(x, y, title, CurrentStyles.Header)

	sub := " Distributed Raft Key-Value Store "
	b.DrawString(x+len(title), y, sub, CurrentStyles.Muted)

	// Connection Status top right
	status := "CONNECTED"
	style := CurrentStyles.Success
	if !model.Connected {
		status = "DISCONNECTED"
		style = CurrentStyles.Error
		if model.ReconnectAttempts > 0 {
			status += fmt.Sprintf(" (Retry %d)", model.ReconnectAttempts)
		}
	}
	b.DrawStringAligned(x, y, w, status, style, 2)

	// Separator
	b.DrawString(x, y+1, strings.Repeat("═", w), CurrentStyles.Border)
}

func (v *View) drawFooter(b *Buffer, x, y, w, h int, model *Model) {
	b.DrawString(x, y, strings.Repeat("─", w), CurrentStyles.Border)

	help := fmt.Sprintf(" [Tab] Next | [S-Tab] Prev | [Enter] Exec | Panel: %s ", model.ActivePanel)
	b.DrawStringAligned(x, y+1, w, help, CurrentStyles.Muted, 1) // Centered
}

// RenderMultiNode draws the multi-node application to the buffer.
func (v *View) RenderMultiNode(b *Buffer, model *MultiNodeModel) {
	width, height := b.Width, b.Height

	// Draw Background
	b.FillRect(0, 0, width, height, ' ', CurrentStyles.Normal)

	// Layout Constants
	headerHeight := 3
	footerHeight := 3
	mainHeight := height - headerHeight - footerHeight

	if mainHeight < 0 {
		mainHeight = 0
	}

	// Header
	v.drawHeaderMultiNode(b, 0, 0, width, headerHeight, model)

	// Main Content Area
	sidebarWidth := width / 4
	if sidebarWidth < 25 {
		sidebarWidth = 25
	}
	if sidebarWidth > width {
		sidebarWidth = width
	}
	contentWidth := width - sidebarWidth

	mainY := headerHeight

	// Sidebar: MultiNode status panel
	v.drawMultiNodeSidebar(b, 0, mainY, sidebarWidth, mainHeight, model)

	// Content Area
	contentX := sidebarWidth

	topRowHeight := mainHeight / 3
	midRowHeight := mainHeight - topRowHeight - 4
	commandHeight := 4

	if topRowHeight < 5 {
		topRowHeight = 5
	}
	if commandHeight < 3 {
		commandHeight = 3
	}

	remaining := mainHeight - commandHeight
	if remaining < 10 {
		topRowHeight = remaining / 2
		midRowHeight = remaining - topRowHeight
	}

	// Metrics & Replication
	metricsWidth := contentWidth / 2
	replWidth := contentWidth - metricsWidth

	// Use embedded model for active node state
	activeModel := model.Model

	v.metricsPanel.Draw(b, contentX, mainY, metricsWidth, topRowHeight, activeModel.ClusterState, activeModel.ActivePanel == PanelMetrics)
	v.replicationPanel.Draw(b, contentX+metricsWidth, mainY, replWidth, topRowHeight, activeModel.ClusterState, activeModel.ActivePanel == PanelReplication)

	// Logs
	v.logsPanel.Draw(b, contentX, mainY+topRowHeight, contentWidth, midRowHeight, activeModel.RecentLogs, activeModel.ActivePanel == PanelLogs)

	// Command
	v.commandPanel.Draw(b, contentX, mainY+topRowHeight+midRowHeight, contentWidth, commandHeight, activeModel.CommandInput, activeModel.CommandOutput, activeModel.ErrorMessage, activeModel.ActivePanel == PanelCommand)

	// Footer
	v.drawFooter(b, 0, height-footerHeight, width, footerHeight, model.Model)
}

func (v *View) drawHeaderMultiNode(b *Buffer, x, y, w, h int, model *MultiNodeModel) {
	b.FillRect(x, y, w, h, ' ', CurrentStyles.Normal)

	title := " ANCHOR CLUSTER "
	b.DrawString(x, y, title, CurrentStyles.Header)

	nodeInfo := fmt.Sprintf(" Node %d/%d (%s) ", model.GetActiveNodeNumber(), model.TotalNodes, model.ActiveNodeID)
	b.DrawString(x+len(title), y, nodeInfo, CurrentStyles.Bold)

	leaderInfo := fmt.Sprintf(" Leader: %s (Term: %d) ", model.ClusterLeaderID, model.ClusterTerm)
	b.DrawString(x+len(title)+len(nodeInfo), y, leaderInfo, CurrentStyles.Muted)

	// Separator
	b.DrawString(x, y+1, strings.Repeat("═", w), CurrentStyles.Border)
}

func (v *View) drawMultiNodeSidebar(b *Buffer, x, y, w, h int, model *MultiNodeModel) {
	// Multi-node sidebar with interactive node list
	style := CurrentStyles.Border
	if model.ActivePanel == PanelStatus {
		style = CurrentStyles.BorderFocus
	}
	b.DrawBox(x, y, w, h, style)
	b.DrawString(x+2, y, " Cluster Nodes ", CurrentStyles.Header)

	contentX, contentY := x+2, y+2

	// Instructions
	b.DrawString(contentX, contentY, "[1-9] Switch Node", CurrentStyles.Muted)

	currentY := contentY + 2
	for i := 1; i <= model.TotalNodes; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		if currentY >= y+h-1 {
			break
		}

		isActive := nodeID == model.ActiveNodeID

		// Status dot
		health := model.GetNodeHealth(nodeID)
		connected := health != nil && health.Connected

		statusStyle := CurrentStyles.Success
		statusChar := '●'
		if !connected {
			statusStyle = CurrentStyles.Error
			statusChar = '○'
		}

		b.Set(contentX, currentY, statusChar, statusStyle)

		// Node Name
		nameStyle := CurrentStyles.Normal
		if isActive {
			nameStyle = CurrentStyles.Selected
		}

		// Check role if known
		role := "?"
		state := model.NodeStates[nodeID]
		if state != nil {
			role = state.LocalRole
			if role == "Leader" {
				nameStyle = CurrentStyles.Warning
				if isActive {
					nameStyle = CurrentStyles.Selected
				}
			}
		}

		line := fmt.Sprintf(" %s (%s)", nodeID, role)
		if isActive {
			// Fill line width for selection
			padded := fmt.Sprintf("%-*s", w-5, line)
			b.DrawString(contentX+1, currentY, padded, nameStyle)
		} else {
			b.DrawString(contentX+1, currentY, line, nameStyle)
		}

		currentY++
	}
}
