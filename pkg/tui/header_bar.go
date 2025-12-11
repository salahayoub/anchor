// Package tui provides header bar rendering for the TUI dashboard.
package tui

import (
	"fmt"
	"strings"
	"time"
)

// HeaderBar renders the cluster overview header.
type HeaderBar struct {
	colorSupport   bool
	unicodeSupport bool
}

// NewHeaderBar creates a header bar renderer.
func NewHeaderBar(colorSupport bool) *HeaderBar {
	return &HeaderBar{
		colorSupport:   colorSupport,
		unicodeSupport: true,
	}
}

// NewHeaderBarWithCapabilities creates a header bar renderer with specified capabilities.
func NewHeaderBarWithCapabilities(colorSupport, unicodeSupport bool) *HeaderBar {
	return &HeaderBar{
		colorSupport:   colorSupport,
		unicodeSupport: unicodeSupport,
	}
}

// Render outputs the header bar content.
// Format: "Node X/N | Leader: nodeY | Term: T"
// When no leader is elected, displays "Leader: (none)"
func (h *HeaderBar) Render(activeNode int, totalNodes int, leaderID string, term uint64) string {
	leaderDisplay := leaderID
	if leaderDisplay == "" {
		leaderDisplay = "(none)"
	}

	return fmt.Sprintf("Node %d/%d | Leader: %s | Term: %d", activeNode, totalNodes, leaderDisplay, term)
}

// RenderWithHealth includes health indicators for all nodes.
// Format: "Node X/N | Leader: nodeY | Term: T | Health: [indicators]"
// For disconnected nodes, shows time since last response.
func (h *HeaderBar) RenderWithHealth(model *MultiNodeModel) string {
	if model == nil {
		return ""
	}

	activeNode := model.GetActiveNodeNumber()
	leaderDisplay := model.ClusterLeaderID
	if leaderDisplay == "" {
		leaderDisplay = "(none)"
	}

	baseHeader := fmt.Sprintf("Node %d/%d | Leader: %s | Term: %d",
		activeNode, model.TotalNodes, leaderDisplay, model.ClusterTerm)

	// Build health indicators for all nodes
	healthIndicators := h.buildHealthIndicators(model)
	if healthIndicators != "" {
		return baseHeader + " | " + healthIndicators
	}

	return baseHeader
}

// buildHealthIndicators builds the health indicator string for all nodes.
func (h *HeaderBar) buildHealthIndicators(model *MultiNodeModel) string {
	if model == nil || model.TotalNodes == 0 {
		return ""
	}

	var indicators []string
	nodeIDs := model.GetAllNodeIDs()

	for _, nodeID := range nodeIDs {
		indicator := h.renderNodeHealthIndicator(model, nodeID)
		indicators = append(indicators, indicator)
	}

	return strings.Join(indicators, " ")
}

// renderNodeHealthIndicator renders a single node's health indicator.
// For disconnected nodes, includes time since last response.
func (h *HeaderBar) renderNodeHealthIndicator(model *MultiNodeModel, nodeID string) string {
	health := model.GetNodeHealth(nodeID)
	if health == nil {
		return h.formatNodeIndicator(nodeID, false, false, 0)
	}

	isLeader := model.ClusterLeaderID == nodeID
	disconnectedDuration := time.Duration(0)

	if !health.Connected {
		disconnectedDuration = model.GetDisconnectedDuration(nodeID)
	}

	return h.formatNodeIndicator(nodeID, health.Connected, isLeader, disconnectedDuration)
}

// formatNodeIndicator formats a single node's health indicator.
// Format: "N:●" for connected, "N:○(Xs)" for disconnected with time
func (h *HeaderBar) formatNodeIndicator(nodeID string, connected bool, isLeader bool, disconnectedDuration time.Duration) string {
	// Extract node number from nodeID (e.g., "node1" -> "1")
	nodeNum := extractNodeNumber(nodeID)

	symbol := h.getHealthSymbol(connected, isLeader)

	if !connected && disconnectedDuration > 0 {
		return fmt.Sprintf("%s:%s(%s)", nodeNum, symbol, formatDuration(disconnectedDuration))
	}

	return fmt.Sprintf("%s:%s", nodeNum, symbol)
}

// getHealthSymbol returns the appropriate health symbol.
func (h *HeaderBar) getHealthSymbol(connected bool, isLeader bool) string {
	if h.unicodeSupport {
		if isLeader {
			return SymbolLeader
		}
		if connected {
			return SymbolConnected
		}
		return SymbolDisconnected
	}

	// ASCII fallback
	if isLeader {
		return SymbolLeaderASCII
	}
	if connected {
		return SymbolConnectedASCII
	}
	return SymbolDisconnectedASCII
}

// extractNodeNumber extracts the number from a node ID (e.g., "node1" -> "1").
func extractNodeNumber(nodeID string) string {
	var num int
	_, err := fmt.Sscanf(nodeID, "node%d", &num)
	if err != nil {
		return nodeID
	}
	return fmt.Sprintf("%d", num)
}

// formatDuration formats a duration for display.
// Shows seconds for < 60s, minutes for < 60m, hours otherwise.
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return "0s"
	}

	seconds := int(d.Seconds())
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	}

	minutes := seconds / 60
	if minutes < 60 {
		return fmt.Sprintf("%dm", minutes)
	}

	hours := minutes / 60
	return fmt.Sprintf("%dh", hours)
}

// GetHealthIndicatorForNode returns the health indicator string for a specific node.
// This is useful for testing and external rendering.
func (h *HeaderBar) GetHealthIndicatorForNode(model *MultiNodeModel, nodeID string) string {
	if model == nil {
		return ""
	}
	return h.renderNodeHealthIndicator(model, nodeID)
}
