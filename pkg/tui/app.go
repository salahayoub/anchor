// Package tui provides the main application controller for the TUI dashboard.
package tui

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gdamore/tcell/v2"
)

// KeyEvent represents a keyboard event.
type KeyEvent struct {
	Key  tcell.Key
	Rune rune
	Mod  tcell.ModMask
}

// App is the main TUI application controller.
type App struct {
	model   *Model
	view    *View
	fetcher DataFetcher
	screen  tcell.Screen

	// Channels
	stopChan chan struct{}
	keyChan  chan KeyEvent

	// Synchronization
	mu      sync.RWMutex
	running bool

	// Reconnection settings
	reconnectInterval time.Duration
	reconnectTimeout  time.Duration

	// Key debouncing for Windows
	lastKeyTime time.Time
	lastKey     tcell.Key
	lastRune    rune
}

// NewApp creates a new TUI application.
func NewApp(fetcher DataFetcher) *App {
	return &App{
		model:             NewModel(),
		view:              NewView(),
		fetcher:           fetcher,
		stopChan:          make(chan struct{}),
		keyChan:           make(chan KeyEvent, 10),
		reconnectInterval: 5 * time.Second,
		reconnectTimeout:  30 * time.Second,
	}
}

// Run starts the TUI application main loop.
// It initializes the terminal, starts event handling, and runs the refresh loop.
// Returns an error if initialization fails or if an unrecoverable error occurs.
func (a *App) Run() error {
	// Initialize screen
	screen, err := tcell.NewScreen()
	if err != nil {
		return fmt.Errorf("failed to create screen: %w", err)
	}

	if err := screen.Init(); err != nil {
		return fmt.Errorf("failed to initialize screen: %w", err)
	}

	// Enable mouse support can cause issues on Windows, ensure it's disabled
	screen.EnableMouse(tcell.MouseMotionEvents)
	screen.DisableMouse()

	a.screen = screen
	a.screen.Clear()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Mark as running
	a.mu.Lock()
	a.running = true
	a.mu.Unlock()

	// Create context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start goroutines
	var wg sync.WaitGroup

	// Event polling goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.pollEvents(ctx)
	}()

	// Refresh loop goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.refreshLoop(ctx)
	}()

	// Reconnection goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.reconnectLoop(ctx)
	}()

	// Initial fetch and render
	a.refresh()
	a.render()

	// Main event loop
	for {
		select {
		case <-a.stopChan:
			cancel()
			wg.Wait()
			a.cleanup()
			return nil

		case <-sigChan:
			cancel()
			wg.Wait()
			a.cleanup()
			return nil

		case event := <-a.keyChan:
			if a.handleKeyEvent(event) {
				cancel()
				wg.Wait()
				a.cleanup()
				return nil
			}
			a.render()
		}
	}
}

// Stop gracefully stops the application.
func (a *App) Stop() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.running {
		close(a.stopChan)
		a.running = false
	}
}

// cleanup restores the terminal state.
func (a *App) cleanup() {
	if a.screen != nil {
		a.screen.Fini()
	}
}

// pollEvents polls for terminal events and sends them to the key channel.
func (a *App) pollEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			ev := a.screen.PollEvent()
			if ev == nil {
				return
			}

			switch e := ev.(type) {
			case *tcell.EventKey:
				select {
				case a.keyChan <- KeyEvent{Key: e.Key(), Rune: e.Rune(), Mod: e.Modifiers()}:
				case <-ctx.Done():
					return
				}
			case *tcell.EventResize:
				a.screen.Sync()
				a.render()
			}
		}
	}
}

// refreshLoop periodically refreshes the cluster state.
func (a *App) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(a.model.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.refresh()
			a.render()
		}
	}
}

// reconnectLoop handles automatic reconnection when disconnected.
func (a *App) reconnectLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(a.reconnectInterval):
			a.mu.RLock()
			connected := a.model.Connected
			a.mu.RUnlock()

			if !connected {
				a.attemptReconnect()
				a.render()
			}
		}
	}
}

// attemptReconnect tries to reconnect to the node.
func (a *App) attemptReconnect() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.model.ReconnectAttempts++
	a.model.LastReconnect = time.Now()

	// Check if we've exceeded the timeout
	if a.model.ReconnectAttempts > int(a.reconnectTimeout/a.reconnectInterval) {
		a.model.ErrorMessage = "Connection failed after 30 seconds. Check that the node is running and accessible."
		return
	}

	if err := a.fetcher.Reconnect(); err != nil {
		a.model.ErrorMessage = fmt.Sprintf("Reconnection attempt %d failed: %v", a.model.ReconnectAttempts, err)
		return
	}

	// Reconnection successful
	a.model.Connected = true
	a.model.ReconnectAttempts = 0
	a.model.ErrorMessage = ""

	// Refresh data after reconnection
	a.refreshLocked()
}

// refresh fetches the latest cluster state and updates the model.
func (a *App) refresh() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refreshLocked()
}

// refreshLocked fetches data without acquiring the lock (caller must hold lock).
func (a *App) refreshLocked() {
	// Check connection status
	if !a.fetcher.IsConnected() {
		a.model.Connected = false
		return
	}

	// Fetch cluster state
	state, err := a.fetcher.FetchClusterState()
	if err != nil {
		a.model.Connected = false
		a.model.ErrorMessage = fmt.Sprintf("Failed to fetch cluster state: %v", err)
		return
	}

	a.model.ClusterState = state
	a.model.Connected = true
	// Don't clear ErrorMessage here - preserve command errors until next command

	// Fetch recent logs
	logs, err := a.fetcher.FetchRecentLogs(10)
	if err != nil {
		// Non-fatal error, just log it
		a.model.ErrorMessage = fmt.Sprintf("Failed to fetch logs: %v", err)
	} else {
		a.model.RecentLogs = logs
	}
}

// render draws the current state to the screen.
func (a *App) render() {
	a.mu.RLock()
	output := a.view.Render(a.model)
	a.mu.RUnlock()

	a.screen.Clear()

	// Draw the output to the screen
	row := 0
	col := 0
	for _, r := range output {
		if r == '\n' {
			row++
			col = 0
			continue
		}
		a.screen.SetContent(col, row, r, nil, tcell.StyleDefault)
		col++
	}

	a.screen.Show()
}

// IsRunning returns whether the application is currently running.
func (a *App) IsRunning() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.running
}

// GetModel returns the current model (for testing).
func (a *App) GetModel() *Model {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.model
}

// handleKeyEvent processes a keyboard event and updates the model.
// Returns true if the application should exit.
// Includes debouncing to handle Windows keyboard repeat issues where
// holding a key generates rapid duplicate events.
func (a *App) handleKeyEvent(event KeyEvent) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Debounce: ignore same key within 200ms to prevent keyboard repeat
	// from triggering multiple actions
	now := time.Now()
	if now.Sub(a.lastKeyTime) < 200*time.Millisecond &&
		a.lastKey == event.Key && a.lastRune == event.Rune {
		return false
	}
	a.lastKeyTime = now
	a.lastKey = event.Key
	a.lastRune = event.Rune

	// Handle Ctrl+C for exit
	if event.Key == tcell.KeyCtrlC {
		return true
	}

	// Handle 'q' for exit (only when not in command panel or input is empty)
	if event.Rune == 'q' && (a.model.ActivePanel != PanelCommand || a.model.CommandInput == "") {
		return true
	}

	// Handle Tab for next panel
	if event.Key == tcell.KeyTab {
		if event.Mod&tcell.ModShift != 0 {
			// Shift+Tab: previous panel
			a.model.PrevPanel()
		} else {
			// Tab: next panel
			a.model.NextPanel()
		}
		return false
	}

	// Handle Shift+Tab (BackTab) for previous panel
	if event.Key == tcell.KeyBacktab {
		a.model.PrevPanel()
		return false
	}

	// Handle 'r' for refresh (only when not in command panel or input is empty)
	if event.Rune == 'r' && (a.model.ActivePanel != PanelCommand || a.model.CommandInput == "") {
		a.refreshLocked()
		return false
	}

	// Handle command panel input
	if a.model.ActivePanel == PanelCommand {
		return a.handleCommandInput(event)
	}

	return false
}

// handleCommandInput processes keyboard input for the command panel.
// Returns true if the application should exit.
func (a *App) handleCommandInput(event KeyEvent) bool {
	switch event.Key {
	case tcell.KeyEnter:
		// Execute command
		a.executeCommand()
		return false

	case tcell.KeyBackspace, tcell.KeyBackspace2:
		// Delete last character
		if len(a.model.CommandInput) > 0 {
			a.model.CommandInput = a.model.CommandInput[:len(a.model.CommandInput)-1]
		}
		return false

	case tcell.KeyEscape:
		// Clear input
		a.model.CommandInput = ""
		a.model.ErrorMessage = ""
		return false

	case tcell.KeyRune:
		// Add character to input
		a.model.CommandInput += string(event.Rune)
		return false
	}

	return false
}

// executeCommand parses and executes the current command input.
func (a *App) executeCommand() {
	input := a.model.CommandInput
	if input == "" {
		return
	}

	// Parse the command
	cmd, err := ParseCommand(input)
	if err != nil {
		a.model.ErrorMessage = err.Error()
		a.model.CommandOutput = ""
		return
	}

	// Execute the command
	switch cmd.Type {
	case CommandGet:
		value, err := a.fetcher.ExecuteGet(cmd.Key)
		if err != nil {
			a.model.ErrorMessage = err.Error()
			a.model.CommandOutput = ""
		} else {
			a.model.CommandOutput = value
			a.model.ErrorMessage = ""
		}

	case CommandPut:
		err := a.fetcher.ExecutePut(cmd.Key, cmd.Value)
		if err != nil {
			// Check if it's a not-leader error
			if isNotLeaderError(err) {
				leaderID := extractLeaderID(err)
				a.model.ErrorMessage = a.view.commandPanel.RenderNotLeaderError(leaderID)
			} else {
				a.model.ErrorMessage = err.Error()
			}
			a.model.CommandOutput = ""
		} else {
			a.model.CommandOutput = "OK"
			a.model.ErrorMessage = ""
		}
	}

	// Clear input after execution
	a.model.CommandInput = ""
}

// isNotLeaderError checks if an error indicates the node is not the leader.
func isNotLeaderError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return contains(errStr, "not leader") || contains(errStr, "not the leader")
}

// extractLeaderID extracts the leader ID from a not-leader error message.
func extractLeaderID(err error) string {
	if err == nil {
		return "unknown"
	}
	// Try to extract leader ID from error message
	// Expected format: "not leader, leader is: <id>" or similar
	errStr := err.Error()
	if idx := indexAfter(errStr, "leader is: "); idx >= 0 {
		return errStr[idx:]
	}
	if idx := indexAfter(errStr, "leader: "); idx >= 0 {
		return errStr[idx:]
	}
	return "unknown"
}

// contains checks if s contains substr (case-insensitive).
func contains(s, substr string) bool {
	sLower := toLower(s)
	substrLower := toLower(substr)
	return indexString(sLower, substrLower) >= 0
}

// toLower converts a string to lowercase.
func toLower(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			result[i] = c + 32
		} else {
			result[i] = c
		}
	}
	return string(result)
}

// indexString returns the index of substr in s, or -1 if not found.
func indexString(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	if len(substr) > len(s) {
		return -1
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// indexAfter returns the index after the first occurrence of substr in s.
func indexAfter(s, substr string) int {
	idx := indexString(s, substr)
	if idx < 0 {
		return -1
	}
	return idx + len(substr)
}

// MultiNodeApp is the TUI application controller for multi-node cluster management.
// It extends the basic App with support for multiple nodes, node switching,
// and command auto-routing.
type MultiNodeApp struct {
	multiModel    *MultiNodeModel
	enhancedView  *EnhancedView
	fetcherPool   *FetcherPool
	commandRouter *CommandRouter
	screen        tcell.Screen

	// Channels
	stopChan chan struct{}
	keyChan  chan KeyEvent

	// Synchronization
	mu      sync.RWMutex
	running bool

	// Terminal dimensions
	terminalWidth  int
	terminalHeight int

	// Reconnection settings
	reconnectInterval time.Duration
	reconnectTimeout  time.Duration

	// Key debouncing for Windows
	lastKeyTime time.Time
	lastKey     tcell.Key
	lastRune    rune
}

// NewMultiNodeApp creates a new multi-node TUI application.
func NewMultiNodeApp(nodeCount int, fetcherPool *FetcherPool) *MultiNodeApp {
	multiModel := NewMultiNodeModel(nodeCount)
	return &MultiNodeApp{
		multiModel:        multiModel,
		enhancedView:      NewEnhancedView(),
		fetcherPool:       fetcherPool,
		commandRouter:     NewCommandRouter(fetcherPool, multiModel),
		stopChan:          make(chan struct{}),
		keyChan:           make(chan KeyEvent, 10),
		reconnectInterval: 5 * time.Second,
		reconnectTimeout:  30 * time.Second,
		terminalWidth:     80,
		terminalHeight:    24,
	}
}

// Run starts the multi-node TUI application main loop.
func (a *MultiNodeApp) Run() error {
	// Initialize screen
	screen, err := tcell.NewScreen()
	if err != nil {
		return fmt.Errorf("failed to create screen: %w", err)
	}

	if err := screen.Init(); err != nil {
		return fmt.Errorf("failed to initialize screen: %w", err)
	}

	// Disable mouse support (can cause issues on Windows)
	screen.EnableMouse(tcell.MouseMotionEvents)
	screen.DisableMouse()

	a.screen = screen
	a.screen.Clear()

	// Get initial terminal size
	a.terminalWidth, a.terminalHeight = screen.Size()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Mark as running
	a.mu.Lock()
	a.running = true
	a.mu.Unlock()

	// Create context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start goroutines
	var wg sync.WaitGroup

	// Event polling goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.pollEvents(ctx)
	}()

	// Refresh loop goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.refreshLoop(ctx)
	}()

	// Initial fetch and render
	a.refreshAllNodes()
	a.render()

	// Main event loop
	for {
		select {
		case <-a.stopChan:
			cancel()
			wg.Wait()
			a.cleanup()
			return nil

		case <-sigChan:
			cancel()
			wg.Wait()
			a.cleanup()
			return nil

		case event := <-a.keyChan:
			if a.handleKeyEvent(event) {
				cancel()
				wg.Wait()
				a.cleanup()
				return nil
			}
			a.render()
		}
	}
}

// Stop gracefully stops the multi-node application.
func (a *MultiNodeApp) Stop() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.running {
		close(a.stopChan)
		a.running = false
	}
}

// cleanup restores the terminal state.
func (a *MultiNodeApp) cleanup() {
	if a.screen != nil {
		a.screen.Fini()
	}
}

// pollEvents polls for terminal events and sends them to the key channel.
func (a *MultiNodeApp) pollEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			ev := a.screen.PollEvent()
			if ev == nil {
				return
			}

			switch e := ev.(type) {
			case *tcell.EventKey:
				select {
				case a.keyChan <- KeyEvent{Key: e.Key(), Rune: e.Rune(), Mod: e.Modifiers()}:
				case <-ctx.Done():
					return
				}
			case *tcell.EventResize:
				a.mu.Lock()
				a.terminalWidth, a.terminalHeight = a.screen.Size()
				a.mu.Unlock()
				a.screen.Sync()
				a.render()
			}
		}
	}
}

// refreshLoop periodically refreshes the cluster state from all nodes.
func (a *MultiNodeApp) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(a.multiModel.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.refreshAllNodes()
			a.render()
		}
	}
}

// refreshAllNodes fetches state from all nodes and updates the model.
func (a *MultiNodeApp) refreshAllNodes() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.fetcherPool == nil {
		return
	}

	// Fetch states from all nodes concurrently
	states := a.fetcherPool.FetchAllStates()
	health := a.fetcherPool.FetchAllHealth()

	// Update model with fetched data
	for nodeID, state := range states {
		a.multiModel.UpdateNodeState(nodeID, state)
	}

	for nodeID, h := range health {
		a.multiModel.UpdateNodeHealth(nodeID, h)
	}

	// Update cluster-wide leader info from any connected node
	for _, state := range states {
		if state != nil && state.LeaderID != "" {
			a.multiModel.ClusterLeaderID = state.LeaderID
			a.multiModel.ClusterTerm = state.CurrentTerm
			break
		}
	}
}

// render draws the current state to the screen.
func (a *MultiNodeApp) render() {
	a.mu.RLock()
	output := a.enhancedView.RenderMultiNodeView(a.multiModel, a.terminalWidth)
	a.mu.RUnlock()

	a.screen.Clear()

	// Draw the output to the screen
	row := 0
	col := 0
	for _, r := range output {
		if r == '\n' {
			row++
			col = 0
			continue
		}
		a.screen.SetContent(col, row, r, nil, tcell.StyleDefault)
		col++
	}

	a.screen.Show()
}

// IsRunning returns whether the application is currently running.
func (a *MultiNodeApp) IsRunning() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.running
}

// GetMultiModel returns the current multi-node model (for testing).
func (a *MultiNodeApp) GetMultiModel() *MultiNodeModel {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.multiModel
}

// handleKeyEvent processes a keyboard event and updates the model.
// Returns true if the application should exit.
func (a *MultiNodeApp) handleKeyEvent(event KeyEvent) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Debounce duplicate key events (Windows keyboard repeat issue)
	now := time.Now()
	if now.Sub(a.lastKeyTime) < 200*time.Millisecond &&
		a.lastKey == event.Key && a.lastRune == event.Rune {
		return false
	}
	a.lastKeyTime = now
	a.lastKey = event.Key
	a.lastRune = event.Rune

	// Handle Ctrl+C for exit
	if event.Key == tcell.KeyCtrlC {
		return true
	}

	// Handle 'q' for exit (only when not in command panel or input is empty)
	if event.Rune == 'q' && (a.multiModel.ActivePanel != PanelCommand || a.multiModel.CommandInput == "") {
		return true
	}

	// Handle number keys 1-9 for node switching
	if event.Rune >= '1' && event.Rune <= '9' {
		nodeNum := int(event.Rune - '0')
		// Only switch if not in command panel with input, or if in command panel but input is empty
		if a.multiModel.ActivePanel != PanelCommand || a.multiModel.CommandInput == "" {
			a.multiModel.SetActiveNodeByNumber(nodeNum)
			return false
		}
	}

	// Handle Tab for next panel
	if event.Key == tcell.KeyTab {
		if event.Mod&tcell.ModShift != 0 {
			a.multiModel.PrevPanel()
		} else {
			a.multiModel.NextPanel()
		}
		return false
	}

	// Handle Shift+Tab (BackTab) for previous panel
	if event.Key == tcell.KeyBacktab {
		a.multiModel.PrevPanel()
		return false
	}

	// Handle 'r' for refresh (only when not in command panel or input is empty)
	if event.Rune == 'r' && (a.multiModel.ActivePanel != PanelCommand || a.multiModel.CommandInput == "") {
		a.refreshAllNodesLocked()
		return false
	}

	// Handle command panel input
	if a.multiModel.ActivePanel == PanelCommand {
		return a.handleCommandInput(event)
	}

	return false
}

// refreshAllNodesLocked fetches state from all nodes (caller must hold lock).
func (a *MultiNodeApp) refreshAllNodesLocked() {
	if a.fetcherPool == nil {
		return
	}

	// Note: This is a simplified version that doesn't use goroutines
	// since we're already holding the lock. In production, you might
	// want to release the lock and use channels for updates.
	states := a.fetcherPool.FetchAllStates()
	health := a.fetcherPool.FetchAllHealth()

	for nodeID, state := range states {
		a.multiModel.UpdateNodeState(nodeID, state)
	}

	for nodeID, h := range health {
		a.multiModel.UpdateNodeHealth(nodeID, h)
	}

	for _, state := range states {
		if state != nil && state.LeaderID != "" {
			a.multiModel.ClusterLeaderID = state.LeaderID
			a.multiModel.ClusterTerm = state.CurrentTerm
			break
		}
	}
}

// handleCommandInput processes keyboard input for the command panel.
// Returns true if the application should exit.
func (a *MultiNodeApp) handleCommandInput(event KeyEvent) bool {
	switch event.Key {
	case tcell.KeyEnter:
		a.executeCommand()
		return false

	case tcell.KeyBackspace, tcell.KeyBackspace2:
		if len(a.multiModel.CommandInput) > 0 {
			a.multiModel.CommandInput = a.multiModel.CommandInput[:len(a.multiModel.CommandInput)-1]
		}
		return false

	case tcell.KeyEscape:
		a.multiModel.CommandInput = ""
		a.multiModel.ErrorMessage = ""
		return false

	case tcell.KeyRune:
		a.multiModel.CommandInput += string(event.Rune)
		return false
	}

	return false
}

// executeCommand parses and executes the current command input using the CommandRouter.
func (a *MultiNodeApp) executeCommand() {
	input := a.multiModel.CommandInput
	if input == "" {
		return
	}

	// Use CommandRouter for command execution with auto-routing
	result := a.commandRouter.Execute(input)

	if result.Error != nil {
		a.multiModel.ErrorMessage = result.Error.Error()
		a.multiModel.CommandOutput = ""
	} else {
		a.multiModel.CommandOutput = result.Value
		a.multiModel.ErrorMessage = ""

		// Show forwarding message if command was routed
		if result.WasRouted && result.Message != "" {
			a.multiModel.CommandOutput = result.Message + "\n" + result.Value
		}
	}

	// Clear input after execution
	a.multiModel.CommandInput = ""
}
