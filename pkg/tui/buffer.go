package tui

import (
	"github.com/gdamore/tcell/v2"
)

// Cell represents a single character in the terminal.
type Cell struct {
	Rune  rune
	Style tcell.Style
}

// Buffer acts as an off-screen render target.
type Buffer struct {
	Cells  [][]Cell
	Width  int
	Height int
}

// NewBuffer creates a new buffer of the specified size.
func NewBuffer(width, height int) *Buffer {
	cells := make([][]Cell, height)
	for y := 0; y < height; y++ {
		cells[y] = make([]Cell, width)
		// Initialize with empty space
		for x := 0; x < width; x++ {
			cells[y][x] = Cell{Rune: ' ', Style: CurrentStyles.Normal}
		}
	}
	return &Buffer{
		Cells:  cells,
		Width:  width,
		Height: height,
	}
}

// SetContent applies the buffer contents to the screen.
func (b *Buffer) ApplyToScreen(screen tcell.Screen, offsetX, offsetY int) {
	for y := 0; y < b.Height; y++ {
		for x := 0; x < b.Width; x++ {
			cell := b.Cells[y][x]
			screen.SetContent(offsetX+x, offsetY+y, cell.Rune, nil, cell.Style)
		}
	}
}

// Set writes a rune to the buffer at the specified coordinates.
func (b *Buffer) Set(x, y int, r rune, style tcell.Style) {
	if x >= 0 && x < b.Width && y >= 0 && y < b.Height {
		b.Cells[y][x] = Cell{Rune: r, Style: style}
	}
}

// DrawString writes a string to the buffer at (x, y).
func (b *Buffer) DrawString(x, y int, s string, style tcell.Style) {
	if y < 0 || y >= b.Height {
		return
	}

	col := x
	for _, r := range s {
		if col >= b.Width {
			break
		}
		if col >= 0 {
			b.Cells[y][col] = Cell{Rune: r, Style: style}
		}
		col++
	}
}

// DrawBox draws a border box.
func (b *Buffer) DrawBox(x, y, w, h int, style tcell.Style) {
	if w < 2 || h < 2 {
		return
	}

	// Corners
	b.Set(x, y, '╭', style)
	b.Set(x+w-1, y, '╮', style)
	b.Set(x, y+h-1, '╰', style)
	b.Set(x+w-1, y+h-1, '╯', style)

	// Horizontal
	for i := 1; i < w-1; i++ {
		b.Set(x+i, y, '─', style)
		b.Set(x+i, y+h-1, '─', style)
	}

	// Vertical
	for i := 1; i < h-1; i++ {
		b.Set(x, y+i, '│', style)
		b.Set(x+w-1, y+i, '│', style)
	}
}

// FillRect fills a rectangle with a specific rune and style.
func (b *Buffer) FillRect(x, y, w, h int, r rune, style tcell.Style) {
	for i := 0; i < h; i++ {
		for j := 0; j < w; j++ {
			b.Set(x+j, y+i, r, style)
		}
	}
}

// Helper for drawing aligned text
func (b *Buffer) DrawStringAligned(x, y, width int, s string, style tcell.Style, align int) {
	spaces := width - len(s)
	if spaces < 0 {
		s = s[:width]
		spaces = 0
	}

	startX := x
	if align == 1 { // Center
		startX = x + spaces/2
	} else if align == 2 { // Right
		startX = x + spaces
	}

	b.DrawString(startX, y, s, style)
}

// DrawBar draws a progress bar.
func (b *Buffer) DrawBar(x, y, width int, percentage float64, fg, bg tcell.Style) {
	filledWidth := int(float64(width) * percentage)
	if filledWidth > width {
		filledWidth = width
	}

	// Draw full blocks
	b.FillRect(x, y, filledWidth, 1, '█', fg)

	// Draw empty background
	b.FillRect(x+filledWidth, y, width-filledWidth, 1, '░', bg)
}
