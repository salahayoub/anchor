// Package types holds shared data structures used across anchor packages.
// Keeps things DRY - no more duplicate type definitions scattered around.
package types

// StatusResponse is the JSON payload returned by the /status endpoint.
// Both the HTTP server and TUI fetcher use this, so it lives here.
type StatusResponse struct {
	NodeID         string           `json:"node_id"`
	Role           string           `json:"role"`
	Term           uint64           `json:"term"`
	CommitIndex    uint64           `json:"commit_index"`
	LeaderID       string           `json:"leader_id"`
	Peers          []PeerStatus     `json:"peers"`
	ReplicationLag map[string]int64 `json:"replication_lag"`
}

// PeerStatus tracks what we know about a peer node.
// Included in StatusResponse to give a full cluster picture.
type PeerStatus struct {
	ID         string `json:"id"`
	Role       string `json:"role"`
	Connected  bool   `json:"connected"`
	MatchIndex uint64 `json:"match_index"`
}
