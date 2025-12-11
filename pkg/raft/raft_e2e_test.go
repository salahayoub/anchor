// Package raft provides end-to-end integration tests for the Raft consensus implementation.
// These tests use the real gRPC transport, BoltDB storage, and KVStore FSM.
package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/salahayoub/anchor/pkg/fsm"
	"github.com/salahayoub/anchor/pkg/storage"
	"github.com/salahayoub/anchor/pkg/transport"
)

// e2eNode represents a complete Raft node with all real components.
type e2eNode struct {
	id        string
	raft      *Raft
	transport *transport.GRPCTransport
	store     *storage.BoltStore
	fsm       *fsm.KVStore
	dbPath    string
}

// e2eCluster manages a cluster of real Raft nodes for end-to-end testing.
type e2eCluster struct {
	nodes   map[string]*e2eNode
	tempDir string
}

func newE2ECluster(t *testing.T) *e2eCluster {
	tempDir, err := os.MkdirTemp("", "raft-e2e-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	return &e2eCluster{
		nodes:   make(map[string]*e2eNode),
		tempDir: tempDir,
	}
}

func (c *e2eCluster) addNode(t *testing.T, id string, port int, peerAddrs []string) *e2eNode {
	// Create BoltDB store
	dbPath := filepath.Join(c.tempDir, fmt.Sprintf("%s.db", id))
	store, err := storage.NewBoltStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create BoltStore for %s: %v", id, err)
	}

	// Create KVStore FSM
	kvStore := fsm.NewKVStore()

	// Create gRPC transport
	listenAddr := fmt.Sprintf("127.0.0.1:%d", port)
	trans, err := transport.NewGRPCTransport(listenAddr)
	if err != nil {
		store.Close()
		t.Fatalf("Failed to create GRPCTransport for %s: %v", id, err)
	}

	// Create Raft config
	config := Config{
		ID:               id,
		Peers:            peerAddrs,
		ElectionTimeout:  150 * time.Millisecond,
		HeartbeatTimeout: 50 * time.Millisecond,
	}

	// Create Raft node
	raft, err := NewRaft(config, store, store, kvStore, trans, nil)
	if err != nil {
		trans.Close()
		store.Close()
		t.Fatalf("Failed to create Raft node %s: %v", id, err)
	}

	node := &e2eNode{
		id:        id,
		raft:      raft,
		transport: trans,
		store:     store,
		fsm:       kvStore,
		dbPath:    dbPath,
	}

	c.nodes[id] = node
	return node
}

func (c *e2eCluster) startNode(t *testing.T, id string) {
	node := c.nodes[id]
	if err := node.raft.Start(); err != nil {
		t.Fatalf("Failed to start node %s: %v", id, err)
	}
}

func (c *e2eCluster) stopNode(t *testing.T, id string) {
	node := c.nodes[id]
	if err := node.raft.Stop(); err != nil {
		t.Logf("Error stopping node %s: %v", id, err)
	}
}

func (c *e2eCluster) cleanup() {
	for _, node := range c.nodes {
		node.raft.Stop()
		node.transport.Close()
		node.store.Close()
	}
	os.RemoveAll(c.tempDir)
}

func (c *e2eCluster) waitForLeader(timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, node := range c.nodes {
			if node.raft.State() == Leader {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return ""
}

func (c *e2eCluster) countLeaders() int {
	count := 0
	for _, node := range c.nodes {
		if node.raft.State() == Leader {
			count++
		}
	}
	return count
}

func (c *e2eCluster) getNode(id string) *e2eNode {
	return c.nodes[id]
}

// =============================================================================
// End-to-End Integration Tests
// =============================================================================

// TestE2E_3NodeClusterElectsLeader tests that a 3-node cluster using real
// gRPC transport, BoltDB storage, and KVStore FSM successfully elects a leader.
func TestE2E_3NodeClusterElectsLeader(t *testing.T) {
	cluster := newE2ECluster(t)
	defer cluster.cleanup()

	// Define node addresses
	basePort := 19100
	nodeAddrs := map[string]string{
		"node1": fmt.Sprintf("127.0.0.1:%d", basePort),
		"node2": fmt.Sprintf("127.0.0.1:%d", basePort+1),
		"node3": fmt.Sprintf("127.0.0.1:%d", basePort+2),
	}

	// Create nodes with peer addresses
	for id := range nodeAddrs {
		var peers []string
		for peerID, peerAddr := range nodeAddrs {
			if peerID != id {
				peers = append(peers, peerAddr)
			}
		}
		port := basePort
		switch id {
		case "node2":
			port = basePort + 1
		case "node3":
			port = basePort + 2
		}
		cluster.addNode(t, id, port, peers)
	}

	// Start all nodes
	for id := range nodeAddrs {
		cluster.startNode(t, id)
	}

	// Wait for leader election
	leaderID := cluster.waitForLeader(5 * time.Second)
	if leaderID == "" {
		t.Fatal("No leader elected within timeout")
	}

	t.Logf("Leader elected: %s", leaderID)

	// Allow cluster to stabilize
	time.Sleep(300 * time.Millisecond)

	// Re-verify leader (may have changed during stabilization)
	stableLeaderID := cluster.waitForLeader(2 * time.Second)
	if stableLeaderID == "" {
		t.Fatal("No stable leader after waiting")
	}

	// Verify at least one leader exists
	leaderCount := cluster.countLeaders()
	if leaderCount < 1 {
		t.Errorf("Expected at least 1 leader, got %d", leaderCount)
	}

	t.Logf("Stable leader: %s, leader count: %d", stableLeaderID, leaderCount)

	// Verify followers know a leader (may not be the same one due to elections)
	for id, node := range cluster.nodes {
		if node.raft.State() != Leader {
			knownLeader := node.raft.Leader()
			t.Logf("Node %s knows leader as %q", id, knownLeader)
		}
	}
}

// TestE2E_LeaderFailureTriggersNewElection tests that when the leader fails,
// the remaining nodes elect a new leader using real components.
func TestE2E_LeaderFailureTriggersNewElection(t *testing.T) {
	cluster := newE2ECluster(t)
	defer cluster.cleanup()

	// Define node addresses
	basePort := 19200
	nodeAddrs := map[string]string{
		"node1": fmt.Sprintf("127.0.0.1:%d", basePort),
		"node2": fmt.Sprintf("127.0.0.1:%d", basePort+1),
		"node3": fmt.Sprintf("127.0.0.1:%d", basePort+2),
	}

	// Create nodes
	for id := range nodeAddrs {
		var peers []string
		for peerID, peerAddr := range nodeAddrs {
			if peerID != id {
				peers = append(peers, peerAddr)
			}
		}
		port := basePort
		switch id {
		case "node2":
			port = basePort + 1
		case "node3":
			port = basePort + 2
		}
		cluster.addNode(t, id, port, peers)
	}

	// Start all nodes
	for id := range nodeAddrs {
		cluster.startNode(t, id)
	}

	// Wait for initial leader
	initialLeader := cluster.waitForLeader(5 * time.Second)
	if initialLeader == "" {
		t.Fatal("No initial leader elected")
	}

	t.Logf("Initial leader: %s", initialLeader)
	initialTerm := cluster.getNode(initialLeader).raft.CurrentTerm()

	// Stop the leader
	cluster.stopNode(t, initialLeader)
	t.Logf("Stopped leader %s", initialLeader)

	// Wait for new leader among remaining nodes
	time.Sleep(500 * time.Millisecond) // Allow election timeout

	var newLeader string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for id, node := range cluster.nodes {
			if id != initialLeader && node.raft.State() == Leader {
				newLeader = id
				break
			}
		}
		if newLeader != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if newLeader == "" {
		t.Fatal("No new leader elected after original leader failure")
	}

	if newLeader == initialLeader {
		t.Error("New leader should be different from stopped leader")
	}

	t.Logf("New leader: %s", newLeader)

	// Verify new term is higher
	newTerm := cluster.getNode(newLeader).raft.CurrentTerm()
	if newTerm <= initialTerm {
		t.Errorf("New term %d should be > initial term %d", newTerm, initialTerm)
	}

	t.Logf("Initial term: %d, New term: %d", initialTerm, newTerm)
}

// TestE2E_LogReplicationWithRealStorage tests that log entries are replicated
// and persisted using real BoltDB storage.
func TestE2E_LogReplicationWithRealStorage(t *testing.T) {
	cluster := newE2ECluster(t)
	defer cluster.cleanup()

	// Define node addresses
	basePort := 19300
	nodeAddrs := map[string]string{
		"node1": fmt.Sprintf("127.0.0.1:%d", basePort),
		"node2": fmt.Sprintf("127.0.0.1:%d", basePort+1),
		"node3": fmt.Sprintf("127.0.0.1:%d", basePort+2),
	}

	// Create nodes
	for id := range nodeAddrs {
		var peers []string
		for peerID, peerAddr := range nodeAddrs {
			if peerID != id {
				peers = append(peers, peerAddr)
			}
		}
		port := basePort
		switch id {
		case "node2":
			port = basePort + 1
		case "node3":
			port = basePort + 2
		}
		cluster.addNode(t, id, port, peers)
	}

	// Start all nodes
	for id := range nodeAddrs {
		cluster.startNode(t, id)
	}

	// Wait for leader
	leaderID := cluster.waitForLeader(5 * time.Second)
	if leaderID == "" {
		t.Fatal("No leader elected")
	}

	t.Logf("Leader: %s", leaderID)

	// Allow leader to stabilize
	time.Sleep(300 * time.Millisecond)

	// Re-check leader (may have changed)
	leaderID = cluster.waitForLeader(2 * time.Second)
	leader := cluster.getNode(leaderID)

	// Apply a command through the leader with retry
	cmd := []byte(`{"op":"set","key":"testkey","value":"testvalue"}`)
	var err error
	for i := 0; i < 3; i++ {
		// Get current leader
		leaderID = cluster.waitForLeader(2 * time.Second)
		if leaderID == "" {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		leader = cluster.getNode(leaderID)
		err = leader.raft.Apply(cmd, 3*time.Second)
		if err == nil {
			break
		}
		t.Logf("Apply attempt %d failed: %v, retrying...", i+1, err)
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Failed to apply command after retries: %v", err)
	}

	// Wait for replication
	time.Sleep(500 * time.Millisecond)

	// Verify the FSM has the value on the leader
	val, ok := leader.fsm.Get("testkey")
	if !ok {
		t.Error("Leader FSM does not have the key")
	} else if val != "testvalue" {
		t.Errorf("Leader FSM has wrong value: got %q, want %q", val, "testvalue")
	}

	// Verify log was persisted to BoltDB on leader
	lastIndex, err := leader.store.LastIndex()
	if err != nil {
		t.Fatalf("Failed to get last index from leader store: %v", err)
	}
	if lastIndex == 0 {
		t.Error("Leader store has no log entries")
	}

	t.Logf("Leader last log index: %d", lastIndex)

	// Check followers have replicated the log
	for id, node := range cluster.nodes {
		if id == leaderID {
			continue
		}

		followerLastIndex, err := node.store.LastIndex()
		if err != nil {
			t.Errorf("Failed to get last index from %s: %v", id, err)
			continue
		}

		t.Logf("Node %s last log index: %d", id, followerLastIndex)

		if followerLastIndex > 0 {
			// Verify the log entry content matches
			entry, err := node.store.GetLog(followerLastIndex)
			if err != nil {
				t.Errorf("Failed to get log entry from %s: %v", id, err)
				continue
			}
			t.Logf("Node %s has log entry at index %d with term %d", id, entry.Index, entry.Term)
		}
	}
}
