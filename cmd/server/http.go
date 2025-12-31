// Package main provides the HTTP server for anchor.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
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
	defaultReadTimeout  = 5 * time.Second

	// HTTP header for specifying read consistency level
	headerConsistency = "X-Consistency"
	// HTTP header for returning the applied index
	headerAppliedIndex = "X-Applied-Index"
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
// It reads the value using the Raft Read method with the specified consistency level.
// The consistency level is specified via the X-Consistency header (default: linearizable).
// Returns 404 for non-existent keys.
// Returns 400 for invalid consistency level.
// Returns 503 for not leader / quorum timeout.
// Returns 504 for read timeout.
func (h *KVHandler) HandleGet(w http.ResponseWriter, r *http.Request) {
	// Extract key from URL path
	key := extractKey(r.URL.Path)
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	// Parse consistency level from header
	consistency, err := parseConsistencyHeader(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create read request
	readReq := &raft.ReadRequest{
		Key:         key,
		Consistency: consistency,
	}

	// Perform read operation
	resp, err := h.raft.Read(readReq, defaultReadTimeout)
	if err != nil {
		handleReadError(w, h.raft, err)
		return
	}

	// Check if key was found
	if !resp.Found {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	// Set response headers
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set(headerConsistency, resp.Consistency.String())
	w.Header().Set(headerAppliedIndex, fmt.Sprintf("%d", resp.AppliedIndex))
	w.WriteHeader(http.StatusOK)
	w.Write(resp.Value)
}

// extractKey extracts the key from a URL path like /kv/{key}.
func extractKey(path string) string {
	// Remove leading slash and "kv/" prefix
	path = strings.TrimPrefix(path, "/")
	path = strings.TrimPrefix(path, "kv/")
	return path
}

// parseConsistencyHeader parses the X-Consistency header value.
// Returns Linearizable if the header is absent or empty.
// Returns an error for invalid values.
func parseConsistencyHeader(r *http.Request) (raft.ConsistencyLevel, error) {
	headerValue := r.Header.Get(headerConsistency)
	if headerValue == "" {
		// Default to Linearizable if header is absent
		return raft.Linearizable, nil
	}

	consistency, err := raft.ParseConsistencyLevel(headerValue)
	if err != nil {
		return 0, fmt.Errorf("invalid consistency level: %s", headerValue)
	}
	return consistency, nil
}

// handleReadError handles read operation errors and returns appropriate HTTP status codes.
// - 503 Service Unavailable: not leader, quorum timeout
// - 504 Gateway Timeout: read timeout
// Includes X-Raft-Leader header with leader hint when available.
func handleReadError(w http.ResponseWriter, r *raft.Raft, err error) {
	// Check for NotLeaderError to get leader hint
	var notLeaderErr *raft.NotLeaderError
	if errors.As(err, &notLeaderErr) {
		if notLeaderErr.LeaderHint != "" {
			w.Header().Set("X-Raft-Leader", notLeaderErr.LeaderHint)
		}
		http.Error(w, "Not the leader", http.StatusServiceUnavailable)
		return
	}

	// Check for ErrNotLeader sentinel error
	if errors.Is(err, raft.ErrNotLeader) {
		leaderID := r.Leader()
		if leaderID != "" {
			w.Header().Set("X-Raft-Leader", leaderID)
		}
		http.Error(w, "Not the leader", http.StatusServiceUnavailable)
		return
	}

	// Check for quorum timeout
	if errors.Is(err, raft.ErrQuorumTimeout) {
		leaderID := r.Leader()
		if leaderID != "" {
			w.Header().Set("X-Raft-Leader", leaderID)
		}
		http.Error(w, "Quorum confirmation timed out", http.StatusServiceUnavailable)
		return
	}

	// Check for read timeout
	if errors.Is(err, raft.ErrReadTimeout) {
		http.Error(w, "Read operation timed out", http.StatusGatewayTimeout)
		return
	}

	// Check for general timeout
	if errors.Is(err, raft.ErrTimeout) {
		http.Error(w, "Request timed out", http.StatusGatewayTimeout)
		return
	}

	// Default to internal server error for other errors
	http.Error(w, err.Error(), http.StatusInternalServerError)
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
