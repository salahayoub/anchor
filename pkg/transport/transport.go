// Package transport provides the networking infrastructure for Raft consensus
// protocol communication. It defines the Transport interface and supporting types
// for sending and receiving RPC requests between Raft nodes.
//
// Thread Safety: Implementations of Transport must be safe for concurrent use
// by multiple goroutines.
package transport

import (
	"errors"

	"github.com/salahayoub/anchor/api"
)

// Error variables for transport operations.
var (
	// ErrTransportClosed is returned when operations are attempted on a closed transport.
	ErrTransportClosed = errors.New("transport is closed")
	// ErrConnectionFailed is returned when a connection to a peer cannot be established.
	ErrConnectionFailed = errors.New("failed to connect to peer")
)

// Transport defines the interface for Raft node communication.
// Implementations must be safe for concurrent use by multiple goroutines.
type Transport interface {
	// Consumer returns a channel for receiving incoming RPC requests.
	// The Raft core reads from this channel to process requests asynchronously.
	Consumer() <-chan RPC

	// LocalAddr returns the address on which this transport listens.
	LocalAddr() string

	// SendRequestVote sends a vote request to the target node.
	SendRequestVote(target string, req *api.VoteRequest) (*api.VoteResponse, error)

	// SendAppendEntries sends an append entries request to the target node.
	SendAppendEntries(target string, req *api.AppendEntriesRequest) (*api.AppendEntriesResponse, error)

	// SendInstallSnapshot sends a snapshot installation request to the target node.
	SendInstallSnapshot(target string, req *api.InstallSnapshotRequest) (*api.InstallSnapshotResponse, error)

	// SendJoinCluster sends a join cluster request to the target node.
	SendJoinCluster(target string, req *api.JoinClusterRequest) (*api.JoinClusterResponse, error)

	// SendRemoveServer sends a remove server request to the target node.
	SendRemoveServer(target string, req *api.RemoveServerRequest) (*api.RemoveServerResponse, error)

	// Connect establishes and pools a connection to the peer address.
	Connect(peerAddr string) error

	// Close shuts down the transport and releases all resources.
	Close() error
}

// RPC represents an incoming RPC request with a channel for the response.
// This decouples the transport layer from the Raft core: the transport receives
// requests and puts them on a channel, the Raft core processes them and sends
// responses back via RespChan. This allows the Raft core to process all events
// through a single select statement.
type RPC struct {
	Request  interface{}
	RespChan chan RPCResponse
}

// RPCResponse wraps the response and any error from processing an RPC request.
type RPCResponse struct {
	// Response is the response to the RPC (VoteResponse, AppendEntriesResponse, or InstallSnapshotResponse)
	Response interface{}

	// Error contains any error that occurred during processing
	Error error
}
