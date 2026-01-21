package tui

import (
	"testing"
	"time"

	"github.com/gdamore/tcell/v2"
)

// TestMultiNodeApp_TabNavigation tests tab navigation in MultiNodeApp.
func TestMultiNodeApp_TabNavigation(t *testing.T) {
	// Create app
	app := NewMultiNodeApp(3, nil) // nil fetcher pool is fine for this test

	// Initial state
	if app.multiModel.ActivePanel != PanelStatus {
		t.Errorf("Expected initial panel Status, got %s", app.multiModel.ActivePanel)
	}

	// Helper to send key
	sendKey := func(key tcell.Key) {
		time.Sleep(250 * time.Millisecond) // Bypass debounce
		app.handleKeyEvent(KeyEvent{Key: key})
	}

	// Press Tab -> Metrics
	sendKey(tcell.KeyTab)
	if app.multiModel.ActivePanel != PanelMetrics {
		t.Errorf("After Tab 1, expected Metrics, got %s", app.multiModel.ActivePanel)
	}

	// Press Tab -> Replication
	sendKey(tcell.KeyTab)
	if app.multiModel.ActivePanel != PanelReplication {
		t.Errorf("After Tab 2, expected Replication, got %s", app.multiModel.ActivePanel)
	}

	// Press Tab -> Logs
	sendKey(tcell.KeyTab)
	if app.multiModel.ActivePanel != PanelLogs {
		t.Errorf("After Tab 3, expected Logs, got %s", app.multiModel.ActivePanel)
	}

	// Test circular
	sendKey(tcell.KeyTab) // Command
	sendKey(tcell.KeyTab) // Status
	if app.multiModel.ActivePanel != PanelStatus {
		t.Errorf("After circle, expected Status, got %s", app.multiModel.ActivePanel)
	}
}
