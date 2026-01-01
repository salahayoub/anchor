package transport

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/salahayoub/anchor/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// defaultConsumerBufferSize is the default buffer size for the consumer channel.
	defaultConsumerBufferSize = 256
)

// GRPCTransport implements Transport using gRPC for network communication.
// It is safe for concurrent use by multiple goroutines.
type GRPCTransport struct {
	// Embed UnimplementedRaftServer for forward compatibility
	api.UnimplementedRaftServer

	localAddr string
	consumer  chan RPC

	// Connection pool: map[peerAddr]*grpc.ClientConn
	connPool sync.Map

	// gRPC server components
	server   *grpc.Server
	listener net.Listener

	// Shutdown coordination
	shutdown   chan struct{}
	shutdownMu sync.Mutex
}

// NewGRPCTransport creates a new GRPCTransport that listens on the given address.
// It starts a gRPC server to handle incoming RPC requests.
func NewGRPCTransport(listenAddr string) (*GRPCTransport, error) {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	t := &GRPCTransport{
		localAddr: listener.Addr().String(),
		consumer:  make(chan RPC, defaultConsumerBufferSize),
		shutdown:  make(chan struct{}),
		listener:  listener,
	}

	t.server = grpc.NewServer()
	api.RegisterRaftServer(t.server, t)

	// Start the gRPC server in a goroutine
	go func() {
		_ = t.server.Serve(listener)
	}()

	return t, nil
}

// Consumer returns a read-only channel for receiving incoming RPC requests.
// The Raft core reads from this channel to process requests asynchronously.
func (t *GRPCTransport) Consumer() <-chan RPC {
	return t.consumer
}

// LocalAddr returns the address on which this transport listens.
func (t *GRPCTransport) LocalAddr() string {
	return t.localAddr
}

// SendRequestVote sends a vote request to the target node.
// It uses the connection pool to reuse connections.
func (t *GRPCTransport) SendRequestVote(target string, req *api.VoteRequest) (*api.VoteResponse, error) {
	conn, err := t.getOrCreateConn(target)
	if err != nil {
		return nil, err
	}

	client := api.NewRaftClient(conn)
	return client.RequestVote(context.Background(), req)
}

// SendAppendEntries sends an append entries request to the target node.
// It uses the connection pool to reuse connections.
func (t *GRPCTransport) SendAppendEntries(target string, req *api.AppendEntriesRequest) (*api.AppendEntriesResponse, error) {
	conn, err := t.getOrCreateConn(target)
	if err != nil {
		return nil, err
	}

	client := api.NewRaftClient(conn)
	return client.AppendEntries(context.Background(), req)
}

// SendInstallSnapshot sends a snapshot installation request to the target node.
// It uses the connection pool to reuse connections.
func (t *GRPCTransport) SendInstallSnapshot(target string, req *api.InstallSnapshotRequest) (*api.InstallSnapshotResponse, error) {
	conn, err := t.getOrCreateConn(target)
	if err != nil {
		return nil, err
	}

	client := api.NewRaftClient(conn)
	return client.InstallSnapshot(context.Background(), req)
}

// SendJoinCluster sends a join cluster request to the target node.
// It uses the connection pool to reuse connections.
func (t *GRPCTransport) SendJoinCluster(target string, req *api.JoinClusterRequest) (*api.JoinClusterResponse, error) {
	conn, err := t.getOrCreateConn(target)
	if err != nil {
		return nil, err
	}

	client := api.NewRaftClient(conn)
	return client.JoinCluster(context.Background(), req)
}

// SendRemoveServer sends a remove server request to the target node.
// It uses the connection pool to reuse connections.
func (t *GRPCTransport) SendRemoveServer(target string, req *api.RemoveServerRequest) (*api.RemoveServerResponse, error) {
	conn, err := t.getOrCreateConn(target)
	if err != nil {
		return nil, err
	}

	client := api.NewRaftClient(conn)
	return client.RemoveServer(context.Background(), req)
}

// SendRead sends a read request to the target node (used for follower read forwarding).
// It uses the connection pool to reuse connections.
func (t *GRPCTransport) SendRead(target string, req *api.ReadRequest) (*api.ReadResponse, error) {
	conn, err := t.getOrCreateConn(target)
	if err != nil {
		return nil, err
	}

	client := api.NewRaftClient(conn)
	return client.Read(context.Background(), req)
}

// Connect establishes and pools a connection to the peer address.
// If a connection already exists for the peer, this is a no-op.
func (t *GRPCTransport) Connect(peerAddr string) error {
	// Check if already connected
	if _, ok := t.connPool.Load(peerAddr); ok {
		return nil
	}

	// Check if transport is closed
	select {
	case <-t.shutdown:
		return ErrTransportClosed
	default:
	}

	// Dial the target address
	conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return ErrConnectionFailed
	}

	// Store connection in pool
	t.connPool.Store(peerAddr, conn)
	return nil
}

// getOrCreateConn returns an existing connection from the pool or creates a new one.
// Uses LoadOrStore to handle the race condition where multiple goroutines try to
// connect to the same peer simultaneously - only one connection is kept.
func (t *GRPCTransport) getOrCreateConn(peerAddr string) (*grpc.ClientConn, error) {
	// Check if transport is closed
	select {
	case <-t.shutdown:
		return nil, ErrTransportClosed
	default:
	}

	// Check for existing connection
	if val, ok := t.connPool.Load(peerAddr); ok {
		return val.(*grpc.ClientConn), nil
	}

	// Dial the target address
	conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, ErrConnectionFailed
	}

	// Store connection in pool (use LoadOrStore to handle race conditions)
	actual, loaded := t.connPool.LoadOrStore(peerAddr, conn)
	if loaded {
		// Another goroutine stored a connection first, close ours and use theirs
		conn.Close()
		return actual.(*grpc.ClientConn), nil
	}

	return conn, nil
}

// Close shuts down the transport and releases all resources.
// It stops the gRPC server gracefully, closes all pooled connections,
// and closes the consumer channel. This method is safe to call multiple times.
func (t *GRPCTransport) Close() error {
	t.shutdownMu.Lock()
	defer t.shutdownMu.Unlock()

	// Check if already closed
	select {
	case <-t.shutdown:
		return nil
	default:
	}

	// Signal shutdown to all goroutines
	close(t.shutdown)

	// Stop gRPC server gracefully (this also closes the listener)
	if t.server != nil {
		t.server.GracefulStop()
	}

	// Close all pooled connections
	t.connPool.Range(func(key, value interface{}) bool {
		if conn, ok := value.(*grpc.ClientConn); ok {
			conn.Close()
		}
		t.connPool.Delete(key)
		return true
	})

	// Close consumer channel
	close(t.consumer)

	return nil
}

// Compile-time check that GRPCTransport implements Transport interface.
var _ Transport = (*GRPCTransport)(nil)

// RequestVote handles incoming vote requests from peers (implements api.RaftServer).
// It wraps the request in an RPC struct and sends it to the consumer channel,
// then waits for the Raft core to process and respond.
func (t *GRPCTransport) RequestVote(ctx context.Context, req *api.VoteRequest) (*api.VoteResponse, error) {
	respChan := make(chan RPCResponse, 1)
	rpc := RPC{
		Request:  req,
		RespChan: respChan,
	}

	// Send to consumer channel
	select {
	case t.consumer <- rpc:
	case <-t.shutdown:
		return nil, ErrTransportClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Wait for response
	select {
	case resp := <-respChan:
		if resp.Error != nil {
			return nil, resp.Error
		}
		voteResp, ok := resp.Response.(*api.VoteResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type: %T", resp.Response)
		}
		return voteResp, nil
	case <-t.shutdown:
		return nil, ErrTransportClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// InstallSnapshot handles incoming snapshot installation requests from peers (implements api.RaftServer).
// It wraps the request in an RPC struct and sends it to the consumer channel,
// then waits for the Raft core to process and respond.
func (t *GRPCTransport) InstallSnapshot(ctx context.Context, req *api.InstallSnapshotRequest) (*api.InstallSnapshotResponse, error) {
	respChan := make(chan RPCResponse, 1)
	rpc := RPC{
		Request:  req,
		RespChan: respChan,
	}

	// Send to consumer channel
	select {
	case t.consumer <- rpc:
	case <-t.shutdown:
		return nil, ErrTransportClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Wait for response
	select {
	case resp := <-respChan:
		if resp.Error != nil {
			return nil, resp.Error
		}
		snapshotResp, ok := resp.Response.(*api.InstallSnapshotResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type: %T", resp.Response)
		}
		return snapshotResp, nil
	case <-t.shutdown:
		return nil, ErrTransportClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// AppendEntries handles incoming append entries requests from peers (implements api.RaftServer).
// It wraps the request in an RPC struct and sends it to the consumer channel,
// then waits for the Raft core to process and respond.
func (t *GRPCTransport) AppendEntries(ctx context.Context, req *api.AppendEntriesRequest) (*api.AppendEntriesResponse, error) {
	respChan := make(chan RPCResponse, 1)
	rpc := RPC{
		Request:  req,
		RespChan: respChan,
	}

	// Send to consumer channel
	select {
	case t.consumer <- rpc:
	case <-t.shutdown:
		return nil, ErrTransportClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Wait for response
	select {
	case resp := <-respChan:
		if resp.Error != nil {
			return nil, resp.Error
		}
		appendResp, ok := resp.Response.(*api.AppendEntriesResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type: %T", resp.Response)
		}
		return appendResp, nil
	case <-t.shutdown:
		return nil, ErrTransportClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// JoinCluster handles incoming join cluster requests from new nodes (implements api.RaftServer).
// It wraps the request in an RPC struct and sends it to the consumer channel,
// then waits for the Raft core to process and respond.
func (t *GRPCTransport) JoinCluster(ctx context.Context, req *api.JoinClusterRequest) (*api.JoinClusterResponse, error) {
	respChan := make(chan RPCResponse, 1)
	rpc := RPC{
		Request:  req,
		RespChan: respChan,
	}

	// Send to consumer channel
	select {
	case t.consumer <- rpc:
	case <-t.shutdown:
		return nil, ErrTransportClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Wait for response
	select {
	case resp := <-respChan:
		if resp.Error != nil {
			return nil, resp.Error
		}
		joinResp, ok := resp.Response.(*api.JoinClusterResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type: %T", resp.Response)
		}
		return joinResp, nil
	case <-t.shutdown:
		return nil, ErrTransportClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// RemoveServer handles incoming remove server requests (implements api.RaftServer).
// It wraps the request in an RPC struct and sends it to the consumer channel,
// then waits for the Raft core to process and respond.
func (t *GRPCTransport) RemoveServer(ctx context.Context, req *api.RemoveServerRequest) (*api.RemoveServerResponse, error) {
	respChan := make(chan RPCResponse, 1)
	rpc := RPC{
		Request:  req,
		RespChan: respChan,
	}

	// Send to consumer channel
	select {
	case t.consumer <- rpc:
	case <-t.shutdown:
		return nil, ErrTransportClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Wait for response
	select {
	case resp := <-respChan:
		if resp.Error != nil {
			return nil, resp.Error
		}
		removeResp, ok := resp.Response.(*api.RemoveServerResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type: %T", resp.Response)
		}
		return removeResp, nil
	case <-t.shutdown:
		return nil, ErrTransportClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}


// Read handles incoming read requests from followers (implements api.RaftServer).
// It wraps the request in an RPC struct and sends it to the consumer channel,
// then waits for the Raft core to process and respond.
func (t *GRPCTransport) Read(ctx context.Context, req *api.ReadRequest) (*api.ReadResponse, error) {
	respChan := make(chan RPCResponse, 1)
	rpc := RPC{
		Request:  req,
		RespChan: respChan,
	}

	// Send to consumer channel
	select {
	case t.consumer <- rpc:
	case <-t.shutdown:
		return nil, ErrTransportClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Wait for response
	select {
	case resp := <-respChan:
		if resp.Error != nil {
			return nil, resp.Error
		}
		readResp, ok := resp.Response.(*api.ReadResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type: %T", resp.Response)
		}
		return readResp, nil
	case <-t.shutdown:
		return nil, ErrTransportClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
