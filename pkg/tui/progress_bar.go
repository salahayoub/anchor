// Package tui provides progress bar rendering for replication lag visualization.
package tui

import (
	"fmt"
	"strings"
)

// Color constants for progress bar (using string representations for terminal output)
const (
	ColorGreen  = "green"
	ColorYellow = "yellow"
	ColorRed    = "red"
)

// ProgressBar renders replication lag as a visual bar.
type ProgressBar struct {
	width        int
	colorSupport bool
}

// NewProgressBar creates a progress bar renderer.
// width specifies the character width of the bar (excluding brackets and percentage).
func NewProgressBar(width int, colorSupport bool) *ProgressBar {
	if width < 1 {
		width = 10 // Default minimum width
	}
	return &ProgressBar{
		width:        width,
		colorSupport: colorSupport,
	}
}

// Render outputs a progress bar for the given percentage and lag.
// Returns format: "[████████░░] 80% (-20)"
func (p *ProgressBar) Render(percentage float64, lag int64) string {
	// Clamp percentage to valid range
	if percentage < 0 {
		percentage = 0
	}
	if percentage > 100 {
		percentage = 100
	}

	// Calculate filled and empty portions
	filledCount := int(percentage / 100 * float64(p.width))
	emptyCount := p.width - filledCount

	// Build the bar
	var sb strings.Builder
	sb.WriteString("[")
	sb.WriteString(strings.Repeat("█", filledCount))
	sb.WriteString(strings.Repeat("░", emptyCount))
	sb.WriteString("]")

	// Add percentage
	sb.WriteString(fmt.Sprintf(" %.0f%%", percentage))

	// Add numeric lag value
	sb.WriteString(fmt.Sprintf(" (-%d)", lag))

	return sb.String()
}

// GetColor returns the appropriate color based on lag.
// lag <= 100: green, 100 < lag <= 500: yellow, lag > 500: red
func (p *ProgressBar) GetColor(lag int64) string {
	if lag <= 100 {
		return ColorGreen
	}
	if lag <= 500 {
		return ColorYellow
	}
	return ColorRed
}

// RenderWithColor outputs a progress bar with color information.
// Returns the rendered bar and the color to use.
func (p *ProgressBar) RenderWithColor(percentage float64, lag int64) (string, string) {
	bar := p.Render(percentage, lag)
	color := p.GetColor(lag)
	return bar, color
}

// CalculatePercentage calculates the replication percentage from leader and follower indices.
// Returns 100 if follower is caught up or ahead, otherwise returns the percentage.
func CalculatePercentage(leaderLastIndex, followerMatchIndex uint64) float64 {
	if leaderLastIndex == 0 {
		return 100.0 // No logs to replicate
	}
	if followerMatchIndex >= leaderLastIndex {
		return 100.0 // Fully replicated
	}
	return float64(followerMatchIndex) / float64(leaderLastIndex) * 100.0
}

// CalculateLag calculates the replication lag (number of entries behind).
func CalculateLag(leaderLastIndex, followerMatchIndex uint64) int64 {
	if followerMatchIndex >= leaderLastIndex {
		return 0
	}
	return int64(leaderLastIndex - followerMatchIndex)
}
