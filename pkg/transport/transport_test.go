// Package transport provides integration tests for the GRPCTransport implementation.
package transport

import (
	"testing"
	"time"

	"github.com/salahayoub/anchor/api"
)

// TestGRPCTransport_EndToEnd_RequestVote tests the complete flow of a RequestVote RPC
// between two transport instances.
// - Transport A listens on a dynamic port
// - Transport B listens on a dynamic port
// - B connects to A
// - B sends RequestVote to A
// - A receives request via Consumer channel
// - A sends response via RespChan
// - B receives response
func TestGRPCTransport_EndToEnd_RequestVote(t *testing.T) {
	// Create Transport A (receiver)
	transportA, err := NewGRPCTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport A: %v", err)
	}
	defer transportA.Close()

	// Create Transport B (sender)
	transportB, err := NewGRPCTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport B: %v", err)
	}
	defer transportB.Close()

	// B connects to A
	err = transportB.Connect(transportA.LocalAddr())
	if err != nil {
		t.Fatalf("Failed to connect B to A: %v", err)
	}

	// Prepare the request
	voteReq := &api.VoteRequest{
		Term:         5,
		CandidateId:  []byte("node-b"),
		LastLogIndex: 10,
		LastLogTerm:  4,
	}

	// Expected response
	expectedResp := &api.VoteResponse{
		Term:        5,
		VoteGranted: true,
	}

	// Start a goroutine to handle incoming RPC on Transport A
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case rpc := <-transportA.Consumer():
			// Verify the request type
			req, ok := rpc.Request.(*api.VoteRequest)
			if !ok {
				t.Errorf("Expected *api.VoteRequest, got %T", rpc.Request)
				return
			}

			// Verify request fields
			if req.Term != voteReq.Term {
				t.Errorf("Term mismatch: expected %d, got %d", voteReq.Term, req.Term)
			}
			if string(req.CandidateId) != string(voteReq.CandidateId) {
				t.Errorf("CandidateId mismatch: expected %s, got %s", voteReq.CandidateId, req.CandidateId)
			}
			if req.LastLogIndex != voteReq.LastLogIndex {
				t.Errorf("LastLogIndex mismatch: expected %d, got %d", voteReq.LastLogIndex, req.LastLogIndex)
			}
			if req.LastLogTerm != voteReq.LastLogTerm {
				t.Errorf("LastLogTerm mismatch: expected %d, got %d", voteReq.LastLogTerm, req.LastLogTerm)
			}

			// Send response via RespChan
			rpc.RespChan <- RPCResponse{
				Response: expectedResp,
			}
		case <-time.After(5 * time.Second):
			t.Error("Timeout waiting for RPC on consumer channel")
		}
	}()

	// B sends RequestVote to A
	resp, err := transportB.SendRequestVote(transportA.LocalAddr(), voteReq)
	if err != nil {
		t.Fatalf("SendRequestVote failed: %v", err)
	}

	// Verify response
	if resp.Term != expectedResp.Term {
		t.Errorf("Response Term mismatch: expected %d, got %d", expectedResp.Term, resp.Term)
	}
	if resp.VoteGranted != expectedResp.VoteGranted {
		t.Errorf("Response VoteGranted mismatch: expected %v, got %v", expectedResp.VoteGranted, resp.VoteGranted)
	}

	// Wait for handler to complete
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Timeout waiting for handler to complete")
	}
}

// TestGRPCTransport_EndToEnd_AppendEntries tests the complete flow of an AppendEntries RPC.
func TestGRPCTransport_EndToEnd_AppendEntries(t *testing.T) {
	// Create Transport A (receiver)
	transportA, err := NewGRPCTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport A: %v", err)
	}
	defer transportA.Close()

	// Create Transport B (sender)
	transportB, err := NewGRPCTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport B: %v", err)
	}
	defer transportB.Close()

	// B connects to A
	err = transportB.Connect(transportA.LocalAddr())
	if err != nil {
		t.Fatalf("Failed to connect B to A: %v", err)
	}

	// Prepare the request
	appendReq := &api.AppendEntriesRequest{
		Term:         7,
		LeaderId:     []byte("leader-b"),
		PrevLogIndex: 15,
		PrevLogTerm:  6,
		LeaderCommit: 14,
		Entries: []*api.LogEntry{
			{Index: 16, Term: 7, Type: api.LogType_LOG_COMMAND, Data: []byte("cmd1")},
		},
	}

	// Expected response
	expectedResp := &api.AppendEntriesResponse{
		Term:         7,
		Success:      true,
		LastLogIndex: 16,
	}

	// Start a goroutine to handle incoming RPC on Transport A
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case rpc := <-transportA.Consumer():
			req, ok := rpc.Request.(*api.AppendEntriesRequest)
			if !ok {
				t.Errorf("Expected *api.AppendEntriesRequest, got %T", rpc.Request)
				return
			}

			// Verify request fields
			if req.Term != appendReq.Term {
				t.Errorf("Term mismatch: expected %d, got %d", appendReq.Term, req.Term)
			}
			if req.PrevLogIndex != appendReq.PrevLogIndex {
				t.Errorf("PrevLogIndex mismatch: expected %d, got %d", appendReq.PrevLogIndex, req.PrevLogIndex)
			}

			// Send response
			rpc.RespChan <- RPCResponse{
				Response: expectedResp,
			}
		case <-time.After(5 * time.Second):
			t.Error("Timeout waiting for RPC on consumer channel")
		}
	}()

	// B sends AppendEntries to A
	resp, err := transportB.SendAppendEntries(transportA.LocalAddr(), appendReq)
	if err != nil {
		t.Fatalf("SendAppendEntries failed: %v", err)
	}

	// Verify response
	if resp.Term != expectedResp.Term {
		t.Errorf("Response Term mismatch: expected %d, got %d", expectedResp.Term, resp.Term)
	}
	if resp.Success != expectedResp.Success {
		t.Errorf("Response Success mismatch: expected %v, got %v", expectedResp.Success, resp.Success)
	}
	if resp.LastLogIndex != expectedResp.LastLogIndex {
		t.Errorf("Response LastLogIndex mismatch: expected %d, got %d", expectedResp.LastLogIndex, resp.LastLogIndex)
	}

	// Wait for handler to complete
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Timeout waiting for handler to complete")
	}
}

// TestGRPCTransport_EndToEnd_InstallSnapshot tests the complete flow of an InstallSnapshot RPC.
func TestGRPCTransport_EndToEnd_InstallSnapshot(t *testing.T) {
	// Create Transport A (receiver)
	transportA, err := NewGRPCTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport A: %v", err)
	}
	defer transportA.Close()

	// Create Transport B (sender)
	transportB, err := NewGRPCTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport B: %v", err)
	}
	defer transportB.Close()

	// B connects to A
	err = transportB.Connect(transportA.LocalAddr())
	if err != nil {
		t.Fatalf("Failed to connect B to A: %v", err)
	}

	// Prepare the request
	snapshotReq := &api.InstallSnapshotRequest{
		Term:              10,
		LeaderId:          []byte("leader-b"),
		LastIncludedIndex: 100,
		LastIncludedTerm:  9,
		Offset:            0,
		Data:              []byte("snapshot-data-chunk"),
		Done:              true,
	}

	// Expected response
	expectedResp := &api.InstallSnapshotResponse{
		Term: 10,
	}

	// Start a goroutine to handle incoming RPC on Transport A
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case rpc := <-transportA.Consumer():
			req, ok := rpc.Request.(*api.InstallSnapshotRequest)
			if !ok {
				t.Errorf("Expected *api.InstallSnapshotRequest, got %T", rpc.Request)
				return
			}

			// Verify request fields
			if req.Term != snapshotReq.Term {
				t.Errorf("Term mismatch: expected %d, got %d", snapshotReq.Term, req.Term)
			}
			if req.LastIncludedIndex != snapshotReq.LastIncludedIndex {
				t.Errorf("LastIncludedIndex mismatch: expected %d, got %d", snapshotReq.LastIncludedIndex, req.LastIncludedIndex)
			}

			// Send response
			rpc.RespChan <- RPCResponse{
				Response: expectedResp,
			}
		case <-time.After(5 * time.Second):
			t.Error("Timeout waiting for RPC on consumer channel")
		}
	}()

	// B sends InstallSnapshot to A
	resp, err := transportB.SendInstallSnapshot(transportA.LocalAddr(), snapshotReq)
	if err != nil {
		t.Fatalf("SendInstallSnapshot failed: %v", err)
	}

	// Verify response
	if resp.Term != expectedResp.Term {
		t.Errorf("Response Term mismatch: expected %d, got %d", expectedResp.Term, resp.Term)
	}

	// Wait for handler to complete
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Timeout waiting for handler to complete")
	}
}

// TestGRPCTransport_ConnectionPooling verifies that connections are reused.
func TestGRPCTransport_ConnectionPooling(t *testing.T) {
	// Create Transport A (receiver)
	transportA, err := NewGRPCTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport A: %v", err)
	}
	defer transportA.Close()

	// Create Transport B (sender)
	transportB, err := NewGRPCTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport B: %v", err)
	}
	defer transportB.Close()

	serverAddr := transportA.LocalAddr()

	// Start a goroutine to handle multiple incoming RPCs
	numRPCs := 5
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < numRPCs; i++ {
			select {
			case rpc := <-transportA.Consumer():
				rpc.RespChan <- RPCResponse{
					Response: &api.VoteResponse{Term: uint64(i), VoteGranted: true},
				}
			case <-time.After(5 * time.Second):
				t.Errorf("Timeout waiting for RPC %d", i)
				return
			}
		}
	}()

	// Send multiple RPCs
	for i := 0; i < numRPCs; i++ {
		req := &api.VoteRequest{Term: uint64(i), CandidateId: []byte("node-b")}
		_, err := transportB.SendRequestVote(serverAddr, req)
		if err != nil {
			t.Fatalf("SendRequestVote %d failed: %v", i, err)
		}
	}

	// Wait for handler to complete
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Timeout waiting for handler to complete")
	}

	// Verify only one connection exists in the pool
	connCount := 0
	transportB.connPool.Range(func(key, value interface{}) bool {
		if key.(string) == serverAddr {
			connCount++
		}
		return true
	})

	if connCount != 1 {
		t.Errorf("Expected 1 connection in pool, got %d", connCount)
	}
}

// TestGRPCTransport_LocalAddr verifies that LocalAddr returns the correct address.
func TestGRPCTransport_LocalAddr(t *testing.T) {
	transport, err := NewGRPCTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}
	defer transport.Close()

	addr := transport.LocalAddr()
	if addr == "" {
		t.Error("LocalAddr returned empty string")
	}

	// Verify it's a valid address format
	if addr == "127.0.0.1:0" {
		t.Error("LocalAddr should return actual bound port, not :0")
	}
}

// TestGRPCTransport_Close verifies that Close properly shuts down the transport.
func TestGRPCTransport_Close(t *testing.T) {
	transportA, err := NewGRPCTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport A: %v", err)
	}

	transportB, err := NewGRPCTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport B: %v", err)
	}

	// Establish a connection
	err = transportB.Connect(transportA.LocalAddr())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Close transport B
	err = transportB.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify consumer channel is closed
	select {
	case _, ok := <-transportB.Consumer():
		if ok {
			t.Error("Consumer channel should be closed")
		}
	default:
		// Channel might be empty but not closed yet, give it a moment
		time.Sleep(100 * time.Millisecond)
		select {
		case _, ok := <-transportB.Consumer():
			if ok {
				t.Error("Consumer channel should be closed")
			}
		default:
			t.Error("Consumer channel should be closed and readable")
		}
	}

	// Verify operations fail after close
	_, err = transportB.SendRequestVote(transportA.LocalAddr(), &api.VoteRequest{Term: 1})
	if err != ErrTransportClosed {
		t.Errorf("Expected ErrTransportClosed, got %v", err)
	}

	// Clean up transport A
	transportA.Close()
}
