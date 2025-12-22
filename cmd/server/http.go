// Package main provides the HTTP server for anchor.
package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/salahayoub/anchor/pkg/fsm"
	"github.com/salahayoub/anchor/pkg/raft"
	"github.com/salahayoub/anchor/pkg/types"
)

const (
	defaultApplyTimeout = 5 * time.Second
)

// KVHandler handles HTTP key-value operations.
type KVHandler struct {
	raft *raft.Raft
	fsm  *fsm.KVStore
}

// NewKVHandler creates a new KVHandler with the given Raft node and KVStore.
func NewKVHandler(r *raft.Raft, kv *fsm.KVStore) *KVHandler {
	return &KVHandler{
		raft: r,
		fsm:  kv,
	}
}

// ServeHTTP routes requests to the appropriate handler based on HTTP method.
func (h *KVHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.HandleGet(w, r)
	case http.MethodPut:
		h.HandlePut(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// HandlePut processes PUT /kv/{key} requests.
// It proposes a set command to the Raft cluster.
// Returns 503 with X-Raft-Leader header if not the leader.
// Returns 200 on successful commit.
func (h *KVHandler) HandlePut(w http.ResponseWriter, r *http.Request) {
	// Extract key from URL path
	key := extractKey(r.URL.Path)
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	// Read request body as value
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	value := string(body)

	// Create command for KVStore
	cmd := fsm.Command{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		http.Error(w, "Failed to encode command", http.StatusInternalServerError)
		return
	}

	// Apply command to Raft
	err = h.raft.Apply(cmdBytes, defaultApplyTimeout)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			// Return 503 with leader hint
			leaderID := h.raft.Leader()
			if leaderID != "" {
				w.Header().Set("X-Raft-Leader", leaderID)
			}
			http.Error(w, "Not the leader", http.StatusServiceUnavailable)
			return
		}
		if errors.Is(err, raft.ErrTimeout) {
			http.Error(w, "Request timed out", http.StatusGatewayTimeout)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Success
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// HandleGet processes GET /kv/{key} requests.
// It returns the current value from the local state machine.
// Returns 404 for non-existent keys.
func (h *KVHandler) HandleGet(w http.ResponseWriter, r *http.Request) {
	// Extract key from URL path
	key := extractKey(r.URL.Path)
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	// Get value from KVStore
	value, ok := h.fsm.Get(key)
	if !ok {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	// Return value
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(value))
}

// extractKey extracts the key from a URL path like /kv/{key}.
func extractKey(path string) string {
	// Remove leading slash and "kv/" prefix
	path = strings.TrimPrefix(path, "/")
	path = strings.TrimPrefix(path, "kv/")
	return path
}

// StatusHandler handles HTTP status requests.
type StatusHandler struct {
	raft   *raft.Raft
	nodeID string
}

// NewStatusHandler creates a new StatusHandler with the given Raft node and node ID.
func NewStatusHandler(r *raft.Raft, nodeID string) *StatusHandler {
	return &StatusHandler{raft: r, nodeID: nodeID}
}

// ServeHTTP handles GET /status requests.
// Returns JSON with current node status including role, term, leader, and peers.
func (h *StatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get current state from Raft node using individual methods
	nodeState := h.raft.State()
	leaderID := h.raft.Leader()
	currentTerm := h.raft.CurrentTerm()
	commitIndex := h.raft.CommitIndex()
	config := h.raft.GetConfiguration()
	matchIndex := h.raft.MatchIndex()

	// Convert NodeState to string
	var roleStr string
	switch nodeState {
	case raft.Leader:
		roleStr = "Leader"
	case raft.Candidate:
		roleStr = "Candidate"
	default:
		roleStr = "Follower"
	}

	// Build response
	resp := types.StatusResponse{
		NodeID:         h.nodeID,
		Role:           roleStr,
		Term:           currentTerm,
		CommitIndex:    commitIndex,
		LeaderID:       leaderID,
		ReplicationLag: make(map[string]int64),
	}

	// Add peer information from configuration
	for _, member := range config.Members {
		peerStatus := types.PeerStatus{
			ID:        member.ID,
			Connected: true, // Assume connected for now
		}
		// Determine peer role based on leader
		if member.ID == leaderID {
			peerStatus.Role = "Leader"
		} else {
			peerStatus.Role = "Follower"
		}
		if mi, ok := matchIndex[member.ID]; ok {
			peerStatus.MatchIndex = mi
		}
		resp.Peers = append(resp.Peers, peerStatus)

		// Calculate replication lag for followers (if we're the leader)
		if roleStr == "Leader" && member.ID != h.nodeID {
			mi := matchIndex[member.ID]
			lag := int64(commitIndex) - int64(mi)
			if lag < 0 {
				lag = 0
			}
			resp.ReplicationLag[member.ID] = lag
		}
	}

	// Return JSON response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}
