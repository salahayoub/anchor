// Package tui provides footer bar rendering for the TUI dashboard.
package tui

import "fmt"

// FooterBar renders the keyboard shortcuts footer.
type FooterBar struct {
	terminalWidth int
}

// NewFooterBar creates a footer bar renderer.
func NewFooterBar(width int) *FooterBar {
	return &FooterBar{
		terminalWidth: width,
	}
}

// SetWidth updates the terminal width for the footer bar.
func (f *FooterBar) SetWidth(width int) {
	f.terminalWidth = width
}

// Render outputs the footer bar content.
// Full (width >= 80): "1-N: Switch Node | Tab: Next Panel | r: Refresh | q: Quit"
// Abbreviated (width < 80): "1-N:Node Tab:Panel r:Ref q:Quit"
// When in command panel: "Enter: Execute | Esc: Clear | Tab: Next Panel"
func (f *FooterBar) Render(totalNodes int, inCommandPanel bool) string {
	if inCommandPanel {
		return f.renderCommandPanelShortcuts()
	}
	return f.renderNormalShortcuts(totalNodes)
}

// renderNormalShortcuts renders shortcuts for normal mode.
func (f *FooterBar) renderNormalShortcuts(totalNodes int) string {
	if f.terminalWidth < 80 {
		return f.renderAbbreviatedShortcuts(totalNodes)
	}
	return f.renderFullShortcuts(totalNodes)
}

// renderFullShortcuts renders full keyboard shortcuts.
// Format: "1-N: Switch Node | Tab: Next Panel | r: Refresh | q: Quit"
func (f *FooterBar) renderFullShortcuts(totalNodes int) string {
	return fmt.Sprintf("1-%d: Switch Node | Tab: Next Panel | r: Refresh | q: Quit", totalNodes)
}

// renderAbbreviatedShortcuts renders abbreviated shortcuts for narrow terminals.
// Format: "1-N:Node Tab:Panel r:Ref q:Quit"
func (f *FooterBar) renderAbbreviatedShortcuts(totalNodes int) string {
	return fmt.Sprintf("1-%d:Node Tab:Panel r:Ref q:Quit", totalNodes)
}

// renderCommandPanelShortcuts renders shortcuts when in command panel.
// Format: "Enter: Execute | Esc: Clear | Tab: Next Panel"
func (f *FooterBar) renderCommandPanelShortcuts() string {
	if f.terminalWidth < 80 {
		return "Enter:Exec Esc:Clear Tab:Panel"
	}
	return "Enter: Execute | Esc: Clear | Tab: Next Panel"
}
