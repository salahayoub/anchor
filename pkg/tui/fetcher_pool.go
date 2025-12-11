// Package tui provides a terminal user interface for monitoring and interacting
// with the Skeg distributed key-value store.
package tui

import (
	"sync"
	"time"
)

// FetcherPool manages DataFetcher instances for multiple nodes.
// It provides concurrent access to fetchers and supports cluster-wide operations.
type FetcherPool struct {
	fetchers map[string]DataFetcher
	mu       sync.RWMutex
}

// NewFetcherPool creates a new empty FetcherPool.
func NewFetcherPool() *FetcherPool {
	return &FetcherPool{
		fetchers: make(map[string]DataFetcher),
	}
}

// AddFetcher adds a fetcher for a specific node.
func (fp *FetcherPool) AddFetcher(nodeID string, fetcher DataFetcher) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.fetchers[nodeID] = fetcher
}

// GetFetcher returns the fetcher for a specific node.
// Returns nil if no fetcher exists for the given node ID.
func (fp *FetcherPool) GetFetcher(nodeID string) DataFetcher {
	fp.mu.RLock()
	defer fp.mu.RUnlock()
	return fp.fetchers[nodeID]
}

// GetLeaderFetcher returns the fetcher for the current leader.
// It queries all fetchers to find the leader and returns the fetcher for that node.
// Returns nil if no leader is known or reachable.
func (fp *FetcherPool) GetLeaderFetcher() DataFetcher {
	fp.mu.RLock()
	defer fp.mu.RUnlock()

	// Query each fetcher to find the leader
	for _, fetcher := range fp.fetchers {
		if fetcher == nil || !fetcher.IsConnected() {
			continue
		}

		state, err := fetcher.FetchClusterState()
		if err != nil || state == nil {
			continue
		}

		if state.LeaderID != "" {
			// Found a leader, return the fetcher for that node
			if leaderFetcher, exists := fp.fetchers[state.LeaderID]; exists {
				return leaderFetcher
			}
		}
	}

	return nil
}

// GetLeaderID returns the current leader ID by querying connected nodes.
// Returns empty string if no leader is known.
func (fp *FetcherPool) GetLeaderID() string {
	fp.mu.RLock()
	defer fp.mu.RUnlock()

	for _, fetcher := range fp.fetchers {
		if fetcher == nil || !fetcher.IsConnected() {
			continue
		}

		state, err := fetcher.FetchClusterState()
		if err != nil || state == nil {
			continue
		}

		if state.LeaderID != "" {
			return state.LeaderID
		}
	}

	return ""
}

// FetchAllStates fetches cluster state from all nodes concurrently.
// Returns a map of node ID to ClusterState. Nodes that fail to respond
// will have nil values in the map.
func (fp *FetcherPool) FetchAllStates() map[string]*ClusterState {
	fp.mu.RLock()
	nodeIDs := make([]string, 0, len(fp.fetchers))
	for nodeID := range fp.fetchers {
		nodeIDs = append(nodeIDs, nodeID)
	}
	fp.mu.RUnlock()

	results := make(map[string]*ClusterState)
	var resultsMu sync.Mutex
	var wg sync.WaitGroup

	for _, nodeID := range nodeIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()

			fetcher := fp.GetFetcher(id)
			if fetcher == nil {
				resultsMu.Lock()
				results[id] = nil
				resultsMu.Unlock()
				return
			}

			state, err := fetcher.FetchClusterState()
			resultsMu.Lock()
			if err != nil {
				results[id] = nil
			} else {
				results[id] = state
			}
			resultsMu.Unlock()
		}(nodeID)
	}

	wg.Wait()
	return results
}

// FetchAllHealth fetches health status from all nodes concurrently.
// Returns a map of node ID to NodeHealth.
func (fp *FetcherPool) FetchAllHealth() map[string]*NodeHealth {
	fp.mu.RLock()
	nodeIDs := make([]string, 0, len(fp.fetchers))
	for nodeID := range fp.fetchers {
		nodeIDs = append(nodeIDs, nodeID)
	}
	fp.mu.RUnlock()

	results := make(map[string]*NodeHealth)
	var resultsMu sync.Mutex
	var wg sync.WaitGroup

	for _, nodeID := range nodeIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()

			fetcher := fp.GetFetcher(id)
			health := &NodeHealth{
				Connected: false,
			}

			if fetcher != nil && fetcher.IsConnected() {
				start := timeNow()
				_, err := fetcher.FetchClusterState()
				elapsed := timeNow().Sub(start)

				if err == nil {
					health.Connected = true
					health.LastResponse = timeNow()
					health.ResponseTimeMs = elapsed.Milliseconds()
				} else {
					health.LastError = err
				}
			}

			resultsMu.Lock()
			results[id] = health
			resultsMu.Unlock()
		}(nodeID)
	}

	wg.Wait()
	return results
}

// timeNow is a variable for testing purposes.
var timeNow = time.Now

// Close closes all fetcher connections.
// Note: The DataFetcher interface doesn't have a Close method,
// so this is a no-op for now but provides the interface for future use.
func (fp *FetcherPool) Close() error {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	// Clear the fetchers map
	fp.fetchers = make(map[string]DataFetcher)
	return nil
}

// NodeCount returns the number of nodes in the pool.
func (fp *FetcherPool) NodeCount() int {
	fp.mu.RLock()
	defer fp.mu.RUnlock()
	return len(fp.fetchers)
}

// GetAllNodeIDs returns all node IDs in the pool.
func (fp *FetcherPool) GetAllNodeIDs() []string {
	fp.mu.RLock()
	defer fp.mu.RUnlock()

	nodeIDs := make([]string, 0, len(fp.fetchers))
	for nodeID := range fp.fetchers {
		nodeIDs = append(nodeIDs, nodeID)
	}
	return nodeIDs
}
