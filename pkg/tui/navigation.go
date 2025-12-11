// Package tui provides navigation functions for the TUI dashboard.
package tui

// PanelOrder defines the circular navigation order of panels.
// Order: Status → Metrics → Replication → Logs → Command → Status
var PanelOrder = []PanelType{
	PanelStatus,
	PanelMetrics,
	PanelReplication,
	PanelLogs,
	PanelCommand,
}

// NextPanel returns the next panel in the circular navigation order.
// Navigation order: Status → Metrics → Replication → Logs → Command → Status
func NextPanel(current PanelType) PanelType {
	return PanelType((int(current) + 1) % PanelCount)
}

// PrevPanel returns the previous panel in the circular navigation order.
// Navigation order: Status → Command → Logs → Replication → Metrics → Status
func PrevPanel(current PanelType) PanelType {
	return PanelType((int(current) - 1 + PanelCount) % PanelCount)
}

// NavigateNext moves the model's active panel to the next panel.
// This is a convenience function that modifies the model in place.
func NavigateNext(model *Model) {
	model.ActivePanel = NextPanel(model.ActivePanel)
}

// NavigatePrev moves the model's active panel to the previous panel.
// This is a convenience function that modifies the model in place.
func NavigatePrev(model *Model) {
	model.ActivePanel = PrevPanel(model.ActivePanel)
}

// GetPanelIndex returns the index of a panel in the navigation order.
func GetPanelIndex(panel PanelType) int {
	return int(panel)
}

// GetPanelByIndex returns the panel at the given index in the navigation order.
// The index wraps around if out of bounds.
func GetPanelByIndex(index int) PanelType {
	// Handle negative indices
	index = ((index % PanelCount) + PanelCount) % PanelCount
	return PanelType(index)
}

// NavigateToPanel sets the model's active panel to the specified panel.
func NavigateToPanel(model *Model, panel PanelType) {
	if panel >= 0 && panel < PanelType(PanelCount) {
		model.ActivePanel = panel
	}
}

// IsValidPanel returns true if the panel type is valid.
func IsValidPanel(panel PanelType) bool {
	return panel >= 0 && panel < PanelType(PanelCount)
}
