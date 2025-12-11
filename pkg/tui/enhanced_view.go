// Package tui provides enhanced view rendering with color and visual indicators.
package tui

// Role constants
const (
	RoleLeader    = "Leader"
	RoleCandidate = "Candidate"
	RoleFollower  = "Follower"
)

// Text fallback indicators for roles when color is not supported
const (
	RoleIndicatorLeader    = "[L]"
	RoleIndicatorCandidate = "[C]"
	RoleIndicatorFollower  = "[F]"
)

// Unicode symbols for health indicators
const (
	SymbolConnected    = "●"
	SymbolDisconnected = "○"
	SymbolLeader       = "★"
)

// ASCII fallback symbols for health indicators
const (
	SymbolConnectedASCII    = "[+]"
	SymbolDisconnectedASCII = "[-]"
	SymbolLeaderASCII       = "[*]"
)

// EnhancedView extends View with color and visual enhancements.
type EnhancedView struct {
	*View
	colorSupport   bool
	unicodeSupport bool
}

// NewEnhancedView creates a view with terminal capability detection.
func NewEnhancedView() *EnhancedView {
	return &EnhancedView{
		View:           NewView(),
		colorSupport:   true, // Default to true, can be overridden
		unicodeSupport: true, // Default to true, can be overridden
	}
}

// NewEnhancedViewWithCapabilities creates a view with specified capabilities.
func NewEnhancedViewWithCapabilities(colorSupport, unicodeSupport bool) *EnhancedView {
	return &EnhancedView{
		View:           NewView(),
		colorSupport:   colorSupport,
		unicodeSupport: unicodeSupport,
	}
}

// SetColorSupport sets whether the terminal supports colors.
func (v *EnhancedView) SetColorSupport(supported bool) {
	v.colorSupport = supported
}

// SetUnicodeSupport sets whether the terminal supports Unicode.
func (v *EnhancedView) SetUnicodeSupport(supported bool) {
	v.unicodeSupport = supported
}

// HasColorSupport returns whether color support is enabled.
func (v *EnhancedView) HasColorSupport() bool {
	return v.colorSupport
}

// HasUnicodeSupport returns whether Unicode support is enabled.
func (v *EnhancedView) HasUnicodeSupport() bool {
	return v.unicodeSupport
}

// RoleColor represents the color for a role.
type RoleColor string

const (
	RoleColorGreen  RoleColor = "green"
	RoleColorYellow RoleColor = "yellow"
	RoleColorWhite  RoleColor = "white"
)

// RenderRoleWithColor renders a role string with appropriate color indicator.
// When colorSupport is true, returns the role with its color.
// When colorSupport is false, returns text indicators ([L], [C], [F]).
func (v *EnhancedView) RenderRoleWithColor(role string) string {
	if !v.colorSupport {
		return v.renderRoleTextFallback(role)
	}
	return role
}

// GetRoleColor returns the color for a given role.
// Leader: green, Candidate: yellow, Follower: white
func (v *EnhancedView) GetRoleColor(role string) RoleColor {
	switch role {
	case RoleLeader:
		return RoleColorGreen
	case RoleCandidate:
		return RoleColorYellow
	case RoleFollower:
		return RoleColorWhite
	default:
		return RoleColorWhite
	}
}

// renderRoleTextFallback renders role with text indicators.
func (v *EnhancedView) renderRoleTextFallback(role string) string {
	switch role {
	case RoleLeader:
		return RoleIndicatorLeader
	case RoleCandidate:
		return RoleIndicatorCandidate
	case RoleFollower:
		return RoleIndicatorFollower
	default:
		return role
	}
}

// RenderRoleWithColorInfo returns the role text and its color.
// This allows callers to apply terminal-specific color codes.
func (v *EnhancedView) RenderRoleWithColorInfo(role string) (string, RoleColor) {
	if !v.colorSupport {
		return v.renderRoleTextFallback(role), RoleColorWhite
	}
	return role, v.GetRoleColor(role)
}

// RenderHealthIndicator renders a health status indicator.
// When unicodeSupport is true: ● (green) for connected, ○ (red) for disconnected, ★ for leader.
// When unicodeSupport is false: [+] for connected, [-] for disconnected, [*] for leader.
func (v *EnhancedView) RenderHealthIndicator(connected bool, isLeader bool) string {
	if v.unicodeSupport {
		return v.renderHealthUnicode(connected, isLeader)
	}
	return v.renderHealthASCII(connected, isLeader)
}

// renderHealthUnicode renders health indicator with Unicode symbols.
func (v *EnhancedView) renderHealthUnicode(connected bool, isLeader bool) string {
	if isLeader {
		return SymbolLeader
	}
	if connected {
		return SymbolConnected
	}
	return SymbolDisconnected
}

// renderHealthASCII renders health indicator with ASCII fallback.
func (v *EnhancedView) renderHealthASCII(connected bool, isLeader bool) string {
	if isLeader {
		return SymbolLeaderASCII
	}
	if connected {
		return SymbolConnectedASCII
	}
	return SymbolDisconnectedASCII
}

// HealthColor represents the color for a health indicator.
type HealthColor string

const (
	HealthColorGreen HealthColor = "green"
	HealthColorRed   HealthColor = "red"
	HealthColorGold  HealthColor = "gold"
)

// GetHealthColor returns the color for a health indicator.
// Connected: green, Disconnected: red, Leader: gold
func (v *EnhancedView) GetHealthColor(connected bool, isLeader bool) HealthColor {
	if isLeader {
		return HealthColorGold
	}
	if connected {
		return HealthColorGreen
	}
	return HealthColorRed
}

// RenderHealthIndicatorWithColor returns the health indicator and its color.
// This allows callers to apply terminal-specific color codes.
func (v *EnhancedView) RenderHealthIndicatorWithColor(connected bool, isLeader bool) (string, HealthColor) {
	indicator := v.RenderHealthIndicator(connected, isLeader)
	color := v.GetHealthColor(connected, isLeader)
	return indicator, color
}

// Election event type constants
const (
	ElectionTypeStarted       = "started"
	ElectionTypeVoteRequested = "vote_requested"
	ElectionTypeLeaderElected = "leader_elected"
)

// ElectionNotification represents a formatted election notification for display.
type ElectionNotification struct {
	Message   string
	NodeID    string
	Term      uint64
	EventType string
	Highlight bool // Whether to highlight/pulse this notification
}

// RenderElectionNotification renders an election event as a notification string.
// Returns a formatted notification containing the relevant node ID and term.
func (v *EnhancedView) RenderElectionNotification(event *ElectionEvent) *ElectionNotification {
	if event == nil {
		return nil
	}

	notification := &ElectionNotification{
		NodeID:    event.NodeID,
		Term:      event.Term,
		EventType: event.Type,
		Highlight: false,
	}

	switch event.Type {
	case ElectionTypeStarted:
		notification.Message = formatElectionStarted(event.NodeID, event.Term)
		notification.Highlight = true
	case ElectionTypeVoteRequested:
		notification.Message = formatVoteRequested(event.NodeID, event.Term)
		notification.Highlight = true
	case ElectionTypeLeaderElected:
		notification.Message = formatLeaderElected(event.NodeID, event.Term)
		notification.Highlight = false
	default:
		notification.Message = formatUnknownElectionEvent(event.Type, event.NodeID, event.Term)
	}

	return notification
}

// formatElectionStarted formats an election started notification.
func formatElectionStarted(nodeID string, term uint64) string {
	return formatNotification("Election started", nodeID, term)
}

// formatVoteRequested formats a vote requested notification.
func formatVoteRequested(nodeID string, term uint64) string {
	return formatNotification("Vote requested by", nodeID, term)
}

// formatLeaderElected formats a leader elected notification.
func formatLeaderElected(nodeID string, term uint64) string {
	return formatNotification("New leader elected", nodeID, term)
}

// formatUnknownElectionEvent formats an unknown election event.
func formatUnknownElectionEvent(eventType, nodeID string, term uint64) string {
	return formatNotification("Election event: "+eventType, nodeID, term)
}

// formatNotification creates a consistent notification format.
// Format: "Message: nodeID (term: T)"
func formatNotification(message, nodeID string, term uint64) string {
	return message + ": " + nodeID + " (term: " + formatUint64(term) + ")"
}

// formatUint64 converts a uint64 to string without using fmt.
func formatUint64(n uint64) string {
	if n == 0 {
		return "0"
	}
	digits := make([]byte, 0, 20)
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// RenderElectionLogEntry renders an election event as a log entry string.
// Format: "[ELECTION] type: nodeID (term: T)"
func (v *EnhancedView) RenderElectionLogEntry(event *ElectionEvent) string {
	if event == nil {
		return ""
	}

	prefix := "[ELECTION] "
	switch event.Type {
	case ElectionTypeStarted:
		return prefix + "started: " + event.NodeID + " (term: " + formatUint64(event.Term) + ")"
	case ElectionTypeVoteRequested:
		return prefix + "vote_requested: " + event.NodeID + " (term: " + formatUint64(event.Term) + ")"
	case ElectionTypeLeaderElected:
		return prefix + "leader_elected: " + event.NodeID + " (term: " + formatUint64(event.Term) + ")"
	default:
		return prefix + event.Type + ": " + event.NodeID + " (term: " + formatUint64(event.Term) + ")"
	}
}

// GetElectionNotificationColor returns the color for an election notification.
// Started/VoteRequested: yellow (warning), LeaderElected: green (success)
func (v *EnhancedView) GetElectionNotificationColor(eventType string) RoleColor {
	switch eventType {
	case ElectionTypeStarted, ElectionTypeVoteRequested:
		return RoleColorYellow
	case ElectionTypeLeaderElected:
		return RoleColorGreen
	default:
		return RoleColorWhite
	}
}

// IsElectionInProgress returns whether an election is currently in progress
// based on the event type.
func IsElectionInProgress(eventType string) bool {
	return eventType == ElectionTypeStarted || eventType == ElectionTypeVoteRequested
}

// EnhancedViewComponents holds the integrated visual components.
type EnhancedViewComponents struct {
	HeaderBar   *HeaderBar
	FooterBar   *FooterBar
	ProgressBar *ProgressBar
}

// NewEnhancedViewWithComponents creates an EnhancedView with all visual components integrated.
// This includes header bar, footer bar, and progress bar renderers.
func NewEnhancedViewWithComponents(terminalWidth int) *EnhancedView {
	colorSupport := DetectColorSupport()
	unicodeSupport := DetectUnicodeSupport()

	ev := &EnhancedView{
		View:           NewView(),
		colorSupport:   colorSupport,
		unicodeSupport: unicodeSupport,
	}

	return ev
}

// GetHeaderBar creates a HeaderBar with the view's terminal capabilities.
func (v *EnhancedView) GetHeaderBar() *HeaderBar {
	return NewHeaderBarWithCapabilities(v.colorSupport, v.unicodeSupport)
}

// GetFooterBar creates a FooterBar with the specified terminal width.
func (v *EnhancedView) GetFooterBar(terminalWidth int) *FooterBar {
	return NewFooterBar(terminalWidth)
}

// GetProgressBar creates a ProgressBar with the view's color support.
func (v *EnhancedView) GetProgressBar(width int) *ProgressBar {
	return NewProgressBar(width, v.colorSupport)
}

// RenderHeaderBar renders the cluster overview header for a MultiNodeModel.
// Format: "Node X/N | Leader: nodeY | Term: T"
func (v *EnhancedView) RenderHeaderBar(model *MultiNodeModel) string {
	if model == nil {
		return ""
	}
	headerBar := v.GetHeaderBar()
	return headerBar.RenderWithHealth(model)
}

// RenderFooterBar renders the keyboard shortcuts footer.
func (v *EnhancedView) RenderFooterBar(model *MultiNodeModel, terminalWidth int) string {
	if model == nil {
		return ""
	}
	footerBar := v.GetFooterBar(terminalWidth)
	inCommandPanel := model.ActivePanel == PanelCommand
	return footerBar.Render(model.TotalNodes, inCommandPanel)
}

// RenderProgressBarForNode renders a progress bar for a specific node's replication lag.
func (v *EnhancedView) RenderProgressBarForNode(leaderLastIndex, followerMatchIndex uint64, width int) string {
	percentage := CalculatePercentage(leaderLastIndex, followerMatchIndex)
	lag := CalculateLag(leaderLastIndex, followerMatchIndex)
	progressBar := v.GetProgressBar(width)
	return progressBar.Render(percentage, lag)
}

// RenderMultiNodeView renders the complete view for a MultiNodeModel.
// This integrates header bar, panels, and footer bar.
func (v *EnhancedView) RenderMultiNodeView(model *MultiNodeModel, terminalWidth int) string {
	if model == nil {
		return ""
	}

	var sb stringBuilder
	sb.init()

	// Render header bar
	header := v.RenderHeaderBar(model)
	if header != "" {
		sb.writeString(header)
		sb.writeString("\n\n")
	}

	// Render election notification if in progress
	if model.ElectionInProgress && model.LastElectionEvent != nil {
		notification := v.RenderElectionNotification(model.LastElectionEvent)
		if notification != nil {
			sb.writeString("*** ")
			sb.writeString(notification.Message)
			sb.writeString(" ***\n\n")
		}
	}

	// Render connection status if disconnected
	if !model.Connected {
		sb.writeString(v.View.RenderConnectionStatus(model.Model))
		sb.writeString("\n")
	}

	// Render all panels using the embedded View
	panels := []PanelType{PanelStatus, PanelMetrics, PanelReplication, PanelLogs, PanelCommand}
	for _, panel := range panels {
		panelContent := v.RenderEnhancedPanel(panel, model)
		sb.writeString(panelContent)
		sb.writeString("\n")
	}

	// Render footer bar
	footer := v.RenderFooterBar(model, terminalWidth)
	if footer != "" {
		sb.writeString("\n")
		sb.writeString(footer)
	}

	return sb.string()
}

// RenderEnhancedPanel renders a single panel with enhanced styling.
// Uses double-line borders for focused panels and single-line for unfocused.
func (v *EnhancedView) RenderEnhancedPanel(panelType PanelType, model *MultiNodeModel) string {
	if model == nil {
		return ""
	}

	// Get the active node's state for rendering
	state := model.GetActiveNodeState()

	var content string
	var title string

	switch panelType {
	case PanelStatus:
		title = "Status"
		content = v.renderEnhancedStatusPanel(state, model)
	case PanelMetrics:
		title = "Metrics"
		content = v.View.metricsPanel.Render(state)
	case PanelReplication:
		title = "Replication"
		content = v.renderEnhancedReplicationPanel(state)
	case PanelLogs:
		title = "Logs"
		content = v.View.logsPanel.Render(model.RecentLogs)
	case PanelCommand:
		title = "Command"
		content = v.View.commandPanel.Render(model.CommandInput, model.CommandOutput, model.ErrorMessage)
	default:
		title = "Unknown"
		content = "Unknown panel type"
	}

	focused := model.ActivePanel == panelType
	rendered, _ := RenderPanelWithBorderAndColor(content, title, focused, v.colorSupport)
	return rendered
}

// renderEnhancedStatusPanel renders the status panel with color-coded roles.
func (v *EnhancedView) renderEnhancedStatusPanel(state *ClusterState, model *MultiNodeModel) string {
	if state == nil {
		return "No cluster state available"
	}

	var sb stringBuilder
	sb.init()
	sb.writeString("=== Node Status ===\n")

	// Render local node with color-coded role
	roleText := v.RenderRoleWithColor(state.LocalRole)
	sb.writeString("Local Node: ")
	sb.writeString(state.LocalNodeID)
	sb.writeString(" (")
	sb.writeString(roleText)
	sb.writeString(")\n")

	sb.writeString("Leader: ")
	sb.writeString(state.LeaderID)
	sb.writeString("\n")

	sb.writeString("\nCluster Nodes:\n")

	for _, node := range state.Nodes {
		isLeader := node.ID == state.LeaderID
		health := model.GetNodeHealth(node.ID)
		connected := health != nil && health.Connected

		// Render health indicator
		healthIndicator := v.RenderHealthIndicator(connected, isLeader)

		// Render role with color
		nodeRoleText := v.RenderRoleWithColor(node.Role)

		sb.writeString("  ")
		sb.writeString(healthIndicator)
		sb.writeString(" ")
		sb.writeString(node.ID)
		sb.writeString(": ")
		sb.writeString(nodeRoleText)
		sb.writeString("\n")
	}

	return sb.string()
}

// renderEnhancedReplicationPanel renders the replication panel with progress bars.
func (v *EnhancedView) renderEnhancedReplicationPanel(state *ClusterState) string {
	if state == nil {
		return "No cluster state available"
	}

	var sb stringBuilder
	sb.init()
	sb.writeString("=== Replication ===\n")

	if state.LocalRole != RoleLeader {
		sb.writeString("Replication data is only available on the leader\n")
		return sb.string()
	}

	sb.writeString("Follower Replication Status:\n")
	for _, node := range state.Nodes {
		if node.ID == state.LocalNodeID {
			continue // Skip self
		}

		lag, hasLag := state.ReplicationLag[node.ID]
		if !hasLag {
			lag = 0
		}

		// Calculate percentage (assuming CommitIndex is the leader's last index)
		percentage := CalculatePercentage(state.CommitIndex, node.MatchIndex)

		// Render progress bar
		progressBar := v.GetProgressBar(10)
		bar := progressBar.Render(percentage, lag)

		sb.writeString("  ")
		sb.writeString(node.ID)
		sb.writeString(": ")
		sb.writeString(bar)
		sb.writeString("\n")
	}

	return sb.string()
}

// DetectColorSupport detects if the terminal supports colors.
// This is a simplified detection that defaults to true.
// In a real implementation, this would check TERM, COLORTERM, etc.
func DetectColorSupport() bool {
	// Default to true for most modern terminals
	// A more sophisticated implementation would check:
	// - TERM environment variable
	// - COLORTERM environment variable
	// - NO_COLOR environment variable
	// - Terminal capabilities via terminfo
	return true
}

// DetectUnicodeSupport detects if the terminal supports Unicode.
// This is a simplified detection that defaults to true.
// In a real implementation, this would check locale settings.
func DetectUnicodeSupport() bool {
	// Default to true for most modern terminals
	// A more sophisticated implementation would check:
	// - LANG environment variable
	// - LC_ALL environment variable
	// - Terminal encoding capabilities
	return true
}

// stringBuilder is a simple string builder to avoid importing strings package.
type stringBuilder struct {
	data []byte
}

func (sb *stringBuilder) init() {
	sb.data = make([]byte, 0, 256)
}

func (sb *stringBuilder) writeString(s string) {
	sb.data = append(sb.data, s...)
}

func (sb *stringBuilder) string() string {
	return string(sb.data)
}
