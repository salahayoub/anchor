package tui

import "github.com/gdamore/tcell/v2"

// Theme defines the color palette for the application.
type Theme struct {
	Primary    tcell.Color
	Secondary  tcell.Color
	Background tcell.Color
	Text       tcell.Color
	Muted      tcell.Color
	Success    tcell.Color
	Warning    tcell.Color
	Error      tcell.Color
	Highlight  tcell.Color
}

// DefaultTheme defines the dark mode colors.
var DefaultTheme = Theme{
	Primary:    tcell.NewRGBColor(99, 102, 241),  // Indigo
	Secondary:  tcell.NewRGBColor(168, 85, 247),  // Purple
	Background: tcell.NewRGBColor(15, 23, 42),    // Slate 900
	Text:       tcell.NewRGBColor(226, 232, 240), // Slate 200
	Muted:      tcell.NewRGBColor(148, 163, 184), // Slate 400
	Success:    tcell.NewRGBColor(34, 197, 94),   // Green 500
	Warning:    tcell.NewRGBColor(234, 179, 8),   // Yellow 500
	Error:      tcell.NewRGBColor(239, 68, 68),   // Red 500
	Highlight:  tcell.NewRGBColor(56, 189, 248),  // Sky 400
}

// Styles container for application-wide styles.
type Styles struct {
	Normal      tcell.Style
	Bold        tcell.Style
	Muted       tcell.Style
	Success     tcell.Style
	Warning     tcell.Style
	Error       tcell.Style
	Header      tcell.Style
	Border      tcell.Style
	BorderFocus tcell.Style
	Selected    tcell.Style
	Highlight   tcell.Style
}

// GetStyles returns the style definitions based on the current theme.
func GetStyles(theme Theme) Styles {
	base := tcell.StyleDefault.Background(theme.Background).Foreground(theme.Text)

	return Styles{
		Normal:      base,
		Bold:        base.Bold(true),
		Muted:       base.Foreground(theme.Muted),
		Success:     base.Foreground(theme.Success),
		Warning:     base.Foreground(theme.Warning),
		Error:       base.Foreground(theme.Error),
		Header:      base.Foreground(theme.Primary).Bold(true),
		Border:      base.Foreground(theme.Muted),
		BorderFocus: base.Foreground(theme.Highlight),
		Selected:    base.Background(theme.Highlight).Foreground(theme.Background),
		Highlight:   base.Foreground(theme.Highlight),
	}
}

// CurrentStyles holds the global styles instance.
var CurrentStyles = GetStyles(DefaultTheme)
