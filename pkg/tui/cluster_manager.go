// Package tui provides the terminal interface for monitoring and managing
// anchor's distributed key-value store cluster.
package tui

import (
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"
)

// NodeProcess represents a spawned Raft node process.
type NodeProcess struct {
	ID       string
	Port     int
	HTTPPort int
	DataDir  string
	Cmd      *exec.Cmd
	Process  *os.Process
	Started  time.Time
	Error    error // Error if the node failed to start
}

// ClusterManager handles spawning and managing multiple node processes.
type ClusterManager struct {
	nodes        []*NodeProcess
	nodeCount    int
	basePort     int
	baseHTTPPort int
	dataDir      string
	serverPath   string // Path to server executable
	mu           sync.RWMutex
	started      bool
}

// NewClusterManager creates a manager for N nodes.
// Parameters:
//   - nodeCount: number of nodes to spawn (2-9)
//   - basePort: starting gRPC port (each node gets basePort + i - 1)
//   - baseHTTPPort: starting HTTP port (each node gets baseHTTPPort + i - 1)
//   - dataDir: base data directory (each node gets dataDir/nodeN)
func NewClusterManager(nodeCount int, basePort, baseHTTPPort int, dataDir string) *ClusterManager {
	// Get the absolute path to the current executable
	// This ensures cluster mode can spawn child processes correctly on Windows
	serverPath, err := os.Executable()
	if err != nil {
		// Fallback to explicit relative path (works on Windows unlike bare "server.exe")
		serverPath = ".\\server.exe"
	}

	return &ClusterManager{
		nodes:        make([]*NodeProcess, 0, nodeCount),
		nodeCount:    nodeCount,
		basePort:     basePort,
		baseHTTPPort: baseHTTPPort,
		dataDir:      dataDir,
		serverPath:   serverPath,
	}
}

// SetServerPath sets the path to the server executable.
// This is useful for testing or when the server is in a different location.
func (cm *ClusterManager) SetServerPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.serverPath = path
}

// Start spawns all node processes.
// Returns error if any node fails to start (partial success allowed).
func (cm *ClusterManager) Start() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.started {
		return fmt.Errorf("cluster already started")
	}

	// Build peer list for all nodes
	peers := cm.buildPeerList()

	var startErrors []error

	for i := 1; i <= cm.nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		port := cm.basePort + i - 1
		httpPort := cm.baseHTTPPort + i - 1
		nodeDataDir := fmt.Sprintf("%s/%s", cm.dataDir, nodeID)

		// Create data directory for this node
		if err := os.MkdirAll(nodeDataDir, 0755); err != nil {
			nodeErr := fmt.Errorf("failed to create data directory for %s: %w", nodeID, err)
			startErrors = append(startErrors, nodeErr)
			cm.nodes = append(cm.nodes, &NodeProcess{
				ID:       nodeID,
				Port:     port,
				HTTPPort: httpPort,
				DataDir:  nodeDataDir,
				Error:    nodeErr,
			})
			continue
		}

		// Build peer addresses excluding this node
		nodePeers := cm.buildNodePeers(i, peers)

		// Build command arguments
		args := []string{
			"--id", nodeID,
			"--port", fmt.Sprintf("%d", port),
			"--http-port", fmt.Sprintf("%d", httpPort),
			"--dir", nodeDataDir,
		}
		if len(nodePeers) > 0 {
			args = append(args, "--peers", nodePeers)
		}

		// Create and start the process
		cmd := exec.Command(cm.serverPath, args...)
		cmd.Stdout = nil // Discard stdout
		cmd.Stderr = nil // Discard stderr

		nodeProcess := &NodeProcess{
			ID:       nodeID,
			Port:     port,
			HTTPPort: httpPort,
			DataDir:  nodeDataDir,
			Cmd:      cmd,
			Started:  time.Now(),
		}

		if err := cmd.Start(); err != nil {
			nodeErr := fmt.Errorf("failed to start %s: %w", nodeID, err)
			startErrors = append(startErrors, nodeErr)
			nodeProcess.Error = nodeErr
		} else {
			nodeProcess.Process = cmd.Process
		}

		cm.nodes = append(cm.nodes, nodeProcess)
	}

	cm.started = true

	// Return combined error if any nodes failed
	if len(startErrors) > 0 {
		return &ClusterStartError{Errors: startErrors}
	}

	return nil
}

// buildPeerList builds the complete peer list for all nodes.
func (cm *ClusterManager) buildPeerList() []string {
	peers := make([]string, cm.nodeCount)
	for i := 1; i <= cm.nodeCount; i++ {
		port := cm.basePort + i - 1
		peers[i-1] = fmt.Sprintf("localhost:%d", port)
	}
	return peers
}

// buildNodePeers builds the peer string for a specific node, excluding itself.
func (cm *ClusterManager) buildNodePeers(nodeNum int, allPeers []string) string {
	var nodePeers []string
	for i, peer := range allPeers {
		if i+1 != nodeNum { // Exclude this node
			nodePeers = append(nodePeers, peer)
		}
	}
	if len(nodePeers) == 0 {
		return ""
	}
	result := nodePeers[0]
	for i := 1; i < len(nodePeers); i++ {
		result += "," + nodePeers[i]
	}
	return result
}

// Stop terminates all node processes gracefully.
func (cm *ClusterManager) Stop() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	var stopErrors []error

	for _, node := range cm.nodes {
		if node.Process == nil {
			continue
		}

		// Try graceful termination first (SIGTERM equivalent on Windows)
		if err := node.Process.Signal(os.Interrupt); err != nil {
			// If interrupt fails, try to kill
			if killErr := node.Process.Kill(); killErr != nil {
				stopErrors = append(stopErrors, fmt.Errorf("failed to stop %s: %w", node.ID, killErr))
				continue
			}
		}

		// Wait for process to exit (with timeout)
		done := make(chan error, 1)
		go func(cmd *exec.Cmd) {
			done <- cmd.Wait()
		}(node.Cmd)

		select {
		case <-done:
			// Process exited
		case <-time.After(5 * time.Second):
			// Force kill if graceful shutdown takes too long
			if err := node.Process.Kill(); err != nil {
				stopErrors = append(stopErrors, fmt.Errorf("failed to force kill %s: %w", node.ID, err))
			}
		}
	}

	cm.started = false

	if len(stopErrors) > 0 {
		return &ClusterStopError{Errors: stopErrors}
	}

	return nil
}

// GetNodes returns the list of managed node processes.
func (cm *ClusterManager) GetNodes() []*NodeProcess {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Return a copy to avoid race conditions
	nodes := make([]*NodeProcess, len(cm.nodes))
	copy(nodes, cm.nodes)
	return nodes
}

// GetNodeByID returns a specific node by ID.
// Returns nil if the node doesn't exist.
func (cm *ClusterManager) GetNodeByID(id string) *NodeProcess {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for _, node := range cm.nodes {
		if node.ID == id {
			return node
		}
	}
	return nil
}

// IsNodeRunning checks if a specific node process is still running.
func (cm *ClusterManager) IsNodeRunning(id string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for _, node := range cm.nodes {
		if node.ID == id {
			if node.Process == nil {
				return false
			}
			// Check if process is still running by sending signal 0
			err := node.Process.Signal(os.Signal(nil))
			return err == nil
		}
	}
	return false
}

// NodeCount returns the configured number of nodes.
func (cm *ClusterManager) NodeCount() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.nodeCount
}

// SuccessfulNodeCount returns the number of nodes that started successfully.
func (cm *ClusterManager) SuccessfulNodeCount() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	count := 0
	for _, node := range cm.nodes {
		if node.Error == nil && node.Process != nil {
			count++
		}
	}
	return count
}

// FailedNodes returns a list of nodes that failed to start.
func (cm *ClusterManager) FailedNodes() []*NodeProcess {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var failed []*NodeProcess
	for _, node := range cm.nodes {
		if node.Error != nil {
			failed = append(failed, node)
		}
	}
	return failed
}

// IsStarted returns whether the cluster has been started.
func (cm *ClusterManager) IsStarted() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.started
}

// ClusterStartError represents errors that occurred during cluster startup.
type ClusterStartError struct {
	Errors []error
}

func (e *ClusterStartError) Error() string {
	if len(e.Errors) == 0 {
		return "cluster start failed"
	}
	if len(e.Errors) == 1 {
		return e.Errors[0].Error()
	}
	return fmt.Sprintf("cluster start failed with %d errors: %v", len(e.Errors), e.Errors[0])
}

// ClusterStopError represents errors that occurred during cluster shutdown.
type ClusterStopError struct {
	Errors []error
}

func (e *ClusterStopError) Error() string {
	if len(e.Errors) == 0 {
		return "cluster stop failed"
	}
	if len(e.Errors) == 1 {
		return e.Errors[0].Error()
	}
	return fmt.Sprintf("cluster stop failed with %d errors: %v", len(e.Errors), e.Errors[0])
}

// GetFailedNodeErrors returns a formatted list of error messages for failed nodes.
// Each message identifies the failed node and the error.
func (cm *ClusterManager) GetFailedNodeErrors() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var errors []string
	for _, node := range cm.nodes {
		if node.Error != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", node.ID, node.Error))
		}
	}
	return errors
}

// FormatStartupStatus returns a human-readable status message about cluster startup.
func (cm *ClusterManager) FormatStartupStatus() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	successful := 0
	failed := 0
	for _, node := range cm.nodes {
		if node.Error == nil && node.Process != nil {
			successful++
		} else {
			failed++
		}
	}

	if failed == 0 {
		return fmt.Sprintf("All %d nodes started successfully", successful)
	}

	if successful == 0 {
		return fmt.Sprintf("Failed to start any nodes (%d failed)", failed)
	}

	return fmt.Sprintf("Started %d/%d nodes (%d failed)", successful, cm.nodeCount, failed)
}
