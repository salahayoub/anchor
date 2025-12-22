// Package tui provides the terminal interface for monitoring and managing
// anchor's distributed key-value store cluster.
package tui

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/salahayoub/anchor/pkg/types"
)

// HTTPDataFetcher implements DataFetcher by connecting to a Raft node via HTTP.
// It provides cluster state, log entries, and key-value operations over HTTP.
type HTTPDataFetcher struct {
	baseURL    string
	nodeID     string
	client     *http.Client
	mu         sync.RWMutex
	connected  bool
	lastState  *ClusterState
	lastUpdate time.Time
}

// NewHTTPDataFetcher creates a new HTTP-based data fetcher.
// baseURL should be the HTTP endpoint of the node (e.g., "http://localhost:8001").
func NewHTTPDataFetcher(baseURL string, nodeID string) *HTTPDataFetcher {
	return &HTTPDataFetcher{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		nodeID:  nodeID,
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
		connected: true, // Assume connected initially
	}
}

// FetchClusterState retrieves current cluster state via HTTP.
func (f *HTTPDataFetcher) FetchClusterState() (*ClusterState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Try to fetch status endpoint
	resp, err := f.client.Get(f.baseURL + "/status")
	if err != nil {
		f.connected = false
		// Return cached state if available
		if f.lastState != nil {
			return f.lastState, nil
		}
		// Return a minimal state indicating disconnection
		return &ClusterState{
			LocalNodeID: f.nodeID,
			LocalRole:   "Unknown",
			LastUpdated: time.Now(),
		}, fmt.Errorf("failed to connect: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		f.connected = false
		// Return cached state if available
		if f.lastState != nil {
			return f.lastState, nil
		}
		return &ClusterState{
			LocalNodeID: f.nodeID,
			LocalRole:   "Unknown",
			LastUpdated: time.Now(),
		}, fmt.Errorf("status endpoint returned %d", resp.StatusCode)
	}

	// Parse response
	var statusResp types.StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&statusResp); err != nil {
		f.connected = false
		return nil, fmt.Errorf("failed to decode status: %w", err)
	}

	f.connected = true
	state := &ClusterState{
		LocalNodeID:    statusResp.NodeID,
		LocalRole:      statusResp.Role,
		CurrentTerm:    statusResp.Term,
		CommitIndex:    statusResp.CommitIndex,
		LeaderID:       statusResp.LeaderID,
		LastUpdated:    time.Now(),
		ReplicationLag: statusResp.ReplicationLag,
	}

	// Build node status list
	for _, peer := range statusResp.Peers {
		state.Nodes = append(state.Nodes, NodeStatus{
			ID:         peer.ID,
			Role:       peer.Role,
			Connected:  peer.Connected,
			MatchIndex: peer.MatchIndex,
		})
	}

	f.lastState = state
	f.lastUpdate = time.Now()

	return state, nil
}

// FetchRecentLogs retrieves the N most recent committed log entries.
// Note: This requires a /logs endpoint on the server.
func (f *HTTPDataFetcher) FetchRecentLogs(count int) ([]LogEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	resp, err := f.client.Get(fmt.Sprintf("%s/logs?count=%d", f.baseURL, count))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch logs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Logs endpoint may not exist, return empty
		return []LogEntry{}, nil
	}

	var logs []LogEntry
	if err := json.NewDecoder(resp.Body).Decode(&logs); err != nil {
		return nil, fmt.Errorf("failed to decode logs: %w", err)
	}

	return logs, nil
}

// ExecuteGet performs a GET operation on the KV store via HTTP.
func (f *HTTPDataFetcher) ExecuteGet(key string) (string, error) {
	resp, err := f.client.Get(f.baseURL + "/kv/" + key)
	if err != nil {
		return "", fmt.Errorf("failed to execute GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("key not found: %s", key)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("GET failed: %s", string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	return string(body), nil
}

// ExecutePut performs a PUT operation on the KV store via HTTP.
func (f *HTTPDataFetcher) ExecutePut(key, value string) error {
	req, err := http.NewRequest(http.MethodPut, f.baseURL+"/kv/"+key, strings.NewReader(value))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute PUT: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusServiceUnavailable {
		// Not the leader
		leaderID := resp.Header.Get("X-Raft-Leader")
		if leaderID != "" {
			return fmt.Errorf("not leader, leader is: %s", leaderID)
		}
		return fmt.Errorf("not leader")
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PUT failed: %s", string(body))
	}

	return nil
}

// IsConnected returns whether the fetcher is connected to the node.
func (f *HTTPDataFetcher) IsConnected() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.connected
}

// Reconnect attempts to reconnect to the node.
func (f *HTTPDataFetcher) Reconnect() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Try a simple health check
	resp, err := f.client.Get(f.baseURL + "/status")
	if err != nil {
		f.connected = false
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		f.connected = true
		return nil
	}

	f.connected = false
	return fmt.Errorf("status check failed: %d", resp.StatusCode)
}

// GetNodeID returns the node ID this fetcher is connected to.
func (f *HTTPDataFetcher) GetNodeID() string {
	return f.nodeID
}
