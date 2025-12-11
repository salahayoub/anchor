// Package tui provides view rendering for the TUI dashboard.
package tui

import "strings"

// BorderStyle defines the characters used for panel borders.
type BorderStyle struct {
	TopLeft     string
	TopRight    string
	BottomLeft  string
	BottomRight string
	Horizontal  string
	Vertical    string
}

// NormalBorder is the default border style for unfocused panels.
// Uses single-line box drawing characters (┌─┐│└┘).
var NormalBorder = BorderStyle{
	TopLeft:     "┌",
	TopRight:    "┐",
	BottomLeft:  "└",
	BottomRight: "┘",
	Horizontal:  "─",
	Vertical:    "│",
}

// FocusedBorder is the border style for focused panels with distinct styling.
// Uses double-line box drawing characters (╔═╗║╚╝).
var FocusedBorder = BorderStyle{
	TopLeft:     "╔",
	TopRight:    "╗",
	BottomLeft:  "╚",
	BottomRight: "╝",
	Horizontal:  "═",
	Vertical:    "║",
}

// BorderColor represents the color for panel borders.
type BorderColor string

const (
	// BorderColorNone indicates no color (default terminal color).
	BorderColorNone BorderColor = ""
	// BorderColorCyan is used for focused panel borders.
	BorderColorCyan BorderColor = "cyan"
)

// PanelBorderInfo contains both the border style and color for a panel.
type PanelBorderInfo struct {
	Style BorderStyle
	Color BorderColor
}

// View handles rendering the model to the terminal.
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

// RenderPanelWithBorder wraps panel content with a border.
// The border style depends on whether the panel has focus.
func RenderPanelWithBorder(content string, title string, focused bool) string {
	border := NormalBorder
	if focused {
		border = FocusedBorder
	}

	lines := strings.Split(content, "\n")

	// Find the maximum line width
	maxWidth := len(title) + 4 // Minimum width to fit title
	for _, line := range lines {
		if len(line) > maxWidth {
			maxWidth = len(line)
		}
	}

	// Add padding
	maxWidth += 2

	var sb strings.Builder

	// Top border with title
	sb.WriteString(border.TopLeft)
	titlePadding := (maxWidth - len(title) - 2) / 2
	sb.WriteString(strings.Repeat(border.Horizontal, titlePadding))
	sb.WriteString(" ")
	sb.WriteString(title)
	sb.WriteString(" ")
	remainingPadding := maxWidth - titlePadding - len(title) - 2
	sb.WriteString(strings.Repeat(border.Horizontal, remainingPadding))
	sb.WriteString(border.TopRight)
	sb.WriteString("\n")

	// Content lines
	for _, line := range lines {
		sb.WriteString(border.Vertical)
		sb.WriteString(" ")
		sb.WriteString(line)
		padding := maxWidth - len(line) - 1
		if padding > 0 {
			sb.WriteString(strings.Repeat(" ", padding))
		}
		sb.WriteString(border.Vertical)
		sb.WriteString("\n")
	}

	// Bottom border
	sb.WriteString(border.BottomLeft)
	sb.WriteString(strings.Repeat(border.Horizontal, maxWidth))
	sb.WriteString(border.BottomRight)
	sb.WriteString("\n")

	return sb.String()
}

// RenderPanel renders a single panel with its content and border.
// The panel is highlighted if it has focus.
func (v *View) RenderPanel(panelType PanelType, model *Model) string {
	var content string
	var title string

	switch panelType {
	case PanelStatus:
		title = "Status"
		content = v.statusPanel.Render(model.ClusterState)
	case PanelMetrics:
		title = "Metrics"
		content = v.metricsPanel.Render(model.ClusterState)
	case PanelReplication:
		title = "Replication"
		content = v.replicationPanel.Render(model.ClusterState)
	case PanelLogs:
		title = "Logs"
		content = v.logsPanel.Render(model.RecentLogs)
	case PanelCommand:
		title = "Command"
		content = v.commandPanel.Render(model.CommandInput, model.CommandOutput, model.ErrorMessage)
	default:
		title = "Unknown"
		content = "Unknown panel type"
	}

	focused := model.ActivePanel == panelType
	return RenderPanelWithBorder(content, title, focused)
}

// Render draws the current model state to the terminal.
// Returns a string representation of all panels with proper focus highlighting.
func (v *View) Render(model *Model) string {
	var sb strings.Builder

	// Render connection status if disconnected
	if !model.Connected {
		sb.WriteString(v.RenderConnectionStatus(model))
		sb.WriteString("\n")
	}

	// Render all panels in order
	panels := []PanelType{PanelStatus, PanelMetrics, PanelReplication, PanelLogs, PanelCommand}
	for _, panel := range panels {
		sb.WriteString(v.RenderPanel(panel, model))
		sb.WriteString("\n")
	}

	return sb.String()
}

// RenderConnectionStatus renders the connection status indicator.
// Shows reconnection attempt count when disconnected.
func (v *View) RenderConnectionStatus(model *Model) string {
	if model.Connected {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("*** DISCONNECTED ***\n")
	if model.ReconnectAttempts > 0 {
		sb.WriteString("Reconnection attempts: ")
		sb.WriteString(strings.Repeat(".", model.ReconnectAttempts))
		sb.WriteString(" (")
		// Convert int to string manually to avoid fmt import
		attempts := model.ReconnectAttempts
		if attempts == 0 {
			sb.WriteString("0")
		} else {
			digits := []byte{}
			for attempts > 0 {
				digits = append([]byte{byte('0' + attempts%10)}, digits...)
				attempts /= 10
			}
			sb.Write(digits)
		}
		sb.WriteString(")")
	}
	return sb.String()
}

// IsPanelFocused returns true if the given panel type has focus in the model.
func (v *View) IsPanelFocused(panelType PanelType, model *Model) bool {
	return model.ActivePanel == panelType
}

// GetBorderStyleForPanel returns the appropriate border style for a panel.
// Returns FocusedBorder if the panel has focus, NormalBorder otherwise.
func (v *View) GetBorderStyleForPanel(panelType PanelType, model *Model) BorderStyle {
	if v.IsPanelFocused(panelType, model) {
		return FocusedBorder
	}
	return NormalBorder
}

// GetBorderInfoForPanel returns the border style and color for a panel.
// Focused panels use double-line borders (╔═╗║╚╝) with cyan color when colorSupport is true.
// Unfocused panels use single-line borders (┌─┐│└┘) with no color.
func GetBorderInfoForPanel(focused bool, colorSupport bool) PanelBorderInfo {
	if focused {
		color := BorderColorNone
		if colorSupport {
			color = BorderColorCyan
		}
		return PanelBorderInfo{
			Style: FocusedBorder,
			Color: color,
		}
	}
	return PanelBorderInfo{
		Style: NormalBorder,
		Color: BorderColorNone,
	}
}

// RenderPanelWithBorderAndColor wraps panel content with a border and returns color info.
// The border style depends on whether the panel has focus.
// When focused and colorSupport is true, the border should be rendered in cyan.
func RenderPanelWithBorderAndColor(content string, title string, focused bool, colorSupport bool) (string, BorderColor) {
	info := GetBorderInfoForPanel(focused, colorSupport)
	border := info.Style

	lines := strings.Split(content, "\n")

	// Find the maximum line width
	maxWidth := len(title) + 4 // Minimum width to fit title
	for _, line := range lines {
		if len(line) > maxWidth {
			maxWidth = len(line)
		}
	}

	// Add padding
	maxWidth += 2

	var sb strings.Builder

	// Top border with title
	sb.WriteString(border.TopLeft)
	titlePadding := (maxWidth - len(title) - 2) / 2
	sb.WriteString(strings.Repeat(border.Horizontal, titlePadding))
	sb.WriteString(" ")
	sb.WriteString(title)
	sb.WriteString(" ")
	remainingPadding := maxWidth - titlePadding - len(title) - 2
	sb.WriteString(strings.Repeat(border.Horizontal, remainingPadding))
	sb.WriteString(border.TopRight)
	sb.WriteString("\n")

	// Content lines
	for _, line := range lines {
		sb.WriteString(border.Vertical)
		sb.WriteString(" ")
		sb.WriteString(line)
		padding := maxWidth - len(line) - 1
		if padding > 0 {
			sb.WriteString(strings.Repeat(" ", padding))
		}
		sb.WriteString(border.Vertical)
		sb.WriteString("\n")
	}

	// Bottom border
	sb.WriteString(border.BottomLeft)
	sb.WriteString(strings.Repeat(border.Horizontal, maxWidth))
	sb.WriteString(border.BottomRight)
	sb.WriteString("\n")

	return sb.String(), info.Color
}
