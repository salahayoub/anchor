// Package tui provides a terminal user interface for monitoring and interacting
// with the Skeg distributed key-value store.
package tui

import (
	"errors"
	"fmt"
)

// Error variables for CommandRouter operations.
var (
	// ErrLeaderUnknown is returned when the leader is unknown and auto-routing is needed.
	ErrLeaderUnknown = errors.New("leader unknown, cannot forward command")
	// ErrLeaderUnreachable is returned when the leader cannot be reached.
	ErrLeaderUnreachable = errors.New("leader unreachable")
	// ErrNoActiveNode is returned when no active node is set.
	ErrNoActiveNode = errors.New("no active node set")
	// ErrFetcherNotFound is returned when no fetcher exists for a node.
	ErrFetcherNotFound = errors.New("fetcher not found for node")
)

// CommandResult represents the result of a command execution.
type CommandResult struct {
	Value     string // Result value (for GET) or success message (for PUT)
	WasRouted bool   // True if command was auto-routed to leader
	RoutedTo  string // Node ID the command was routed to (if routed)
	Message   string // Additional message (e.g., forwarding notification)
	Error     error  // Error if command failed
}

// CommandRouter handles routing commands to appropriate nodes.
// It supports auto-routing of write commands to the leader.
type CommandRouter struct {
	fetcherPool *FetcherPool
	model       *MultiNodeModel
}

// NewCommandRouter creates a router with the given fetcher pool and model.
func NewCommandRouter(pool *FetcherPool, model *MultiNodeModel) *CommandRouter {
	return &CommandRouter{
		fetcherPool: pool,
		model:       model,
	}
}

// ExecuteGet executes a GET command on the active node.
// GET commands are always executed on the active node (reads are local).
func (r *CommandRouter) ExecuteGet(key string) *CommandResult {
	if r.model == nil {
		return &CommandResult{Error: ErrNoActiveNode}
	}

	activeNodeID := r.model.ActiveNodeID
	if activeNodeID == "" {
		return &CommandResult{Error: ErrNoActiveNode}
	}

	if r.fetcherPool == nil {
		return &CommandResult{Error: ErrFetcherNotFound}
	}

	fetcher := r.fetcherPool.GetFetcher(activeNodeID)
	if fetcher == nil {
		return &CommandResult{Error: fmt.Errorf("%w: %s", ErrFetcherNotFound, activeNodeID)}
	}

	value, err := fetcher.ExecuteGet(key)
	if err != nil {
		return &CommandResult{Error: err}
	}

	return &CommandResult{
		Value:     value,
		WasRouted: false,
	}
}

// ExecutePut executes a PUT command, auto-routing to leader if needed.
// If the active node is not the leader, the command is forwarded to the leader.
func (r *CommandRouter) ExecutePut(key, value string) *CommandResult {
	if r.model == nil {
		return &CommandResult{Error: ErrNoActiveNode}
	}

	activeNodeID := r.model.ActiveNodeID
	if activeNodeID == "" {
		return &CommandResult{Error: ErrNoActiveNode}
	}

	if r.fetcherPool == nil {
		return &CommandResult{Error: ErrFetcherNotFound}
	}

	// Get the current leader ID
	leaderID := r.GetLeaderID()

	// Check if active node is the leader
	if activeNodeID == leaderID {
		// Execute directly on active node (which is the leader)
		fetcher := r.fetcherPool.GetFetcher(activeNodeID)
		if fetcher == nil {
			return &CommandResult{Error: fmt.Errorf("%w: %s", ErrFetcherNotFound, activeNodeID)}
		}

		err := fetcher.ExecutePut(key, value)
		if err != nil {
			return &CommandResult{Error: err}
		}

		return &CommandResult{
			Value:     fmt.Sprintf("OK: %s = %s", key, value),
			WasRouted: false,
		}
	}

	// Need to auto-route to leader
	if leaderID == "" {
		// Leader unknown - cannot forward
		return &CommandResult{Error: ErrLeaderUnknown}
	}

	// Get the leader's fetcher
	leaderFetcher := r.fetcherPool.GetFetcher(leaderID)
	if leaderFetcher == nil {
		// Leader fetcher not found
		return &CommandResult{Error: fmt.Errorf("%w: %s", ErrLeaderUnreachable, leaderID)}
	}

	// Check if leader is reachable
	if !leaderFetcher.IsConnected() {
		return &CommandResult{Error: fmt.Errorf("%w: %s", ErrLeaderUnreachable, leaderID)}
	}

	// Execute on leader
	err := leaderFetcher.ExecutePut(key, value)
	if err != nil {
		return &CommandResult{Error: err}
	}
	// Display forwarding message and result
	return &CommandResult{
		Value:     fmt.Sprintf("OK: %s = %s", key, value),
		WasRouted: true,
		RoutedTo:  leaderID,
		Message:   fmt.Sprintf("Command forwarded to leader (%s)", leaderID),
	}
}

// GetLeaderID returns the current leader ID by querying the model or fetcher pool.
func (r *CommandRouter) GetLeaderID() string {
	// First check the model's cached leader ID
	if r.model != nil && r.model.ClusterLeaderID != "" {
		return r.model.ClusterLeaderID
	}

	// Fall back to querying the fetcher pool
	if r.fetcherPool != nil {
		return r.fetcherPool.GetLeaderID()
	}

	return ""
}

// Execute parses and executes a command string.
// This is a convenience method that combines parsing and execution.
func (r *CommandRouter) Execute(input string) *CommandResult {
	cmd, err := ParseCommand(input)
	if err != nil {
		return &CommandResult{Error: err}
	}

	switch cmd.Type {
	case CommandGet:
		return r.ExecuteGet(cmd.Key)
	case CommandPut:
		return r.ExecutePut(cmd.Key, cmd.Value)
	default:
		return &CommandResult{Error: ErrUnknownCommand}
	}
}

// IsActiveNodeLeader returns true if the active node is the current leader.
func (r *CommandRouter) IsActiveNodeLeader() bool {
	if r.model == nil {
		return false
	}
	return r.model.ActiveNodeID == r.GetLeaderID()
}
