// Package main provides the CLI server for the Raft consensus implementation.
// It initializes all components and wires them together to create a running Raft node.
//
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/salahayoub/anchor/pkg/fsm"
	"github.com/salahayoub/anchor/pkg/raft"
	"github.com/salahayoub/anchor/pkg/storage"
	"github.com/salahayoub/anchor/pkg/transport"
	"github.com/salahayoub/anchor/pkg/tui"
)

const (
	// defaultElectionTimeout is the base election timeout for Raft.
	defaultElectionTimeout = 150 * time.Millisecond
	// defaultHeartbeatTimeout is the interval at which the leader sends heartbeats.
	defaultHeartbeatTimeout = 50 * time.Millisecond
	// raftDBFilename is the name of the BoltDB file for Raft storage.
	raftDBFilename = "raft.db"
)

func main() {
	// Parse command-line flags
	cfg, err := ParseFlags(flag.CommandLine, os.Args[1:])
	if err != nil {
		log.Fatalf("Failed to parse flags: %v", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Configuration validation failed: %v", err)
	}

	// Check if cluster mode is enabled
	if cfg.IsClusterMode() {
		runClusterMode(cfg)
		return
	}

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory %s: %v", cfg.DataDir, err)
	}

	// Initialize BoltStore at dataDir/raft.db
	dbPath := filepath.Join(cfg.DataDir, raftDBFilename)
	boltStore, err := storage.NewBoltStore(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize BoltStore at %s: %v", dbPath, err)
	}
	// Note: boltStore.Close() is called in gracefulShutdown
	log.Printf("Initialized BoltStore at %s", dbPath)

	// Initialize FileSnapshotStore at dataDir/snapshots
	snapshotDir := filepath.Join(cfg.DataDir, "snapshots")
	snapshotStore, err := raft.NewFileSnapshotStore(snapshotDir)
	if err != nil {
		log.Fatalf("Failed to initialize SnapshotStore at %s: %v", snapshotDir, err)
	}
	log.Printf("Initialized SnapshotStore at %s", snapshotDir)

	// Initialize KVStore as the state machine
	kvStore := fsm.NewKVStore()
	log.Printf("Initialized KVStore state machine")

	// Initialize GRPCTransport on specified port
	listenAddr := fmt.Sprintf(":%d", cfg.Port)
	grpcTransport, err := transport.NewGRPCTransport(listenAddr)
	if err != nil {
		log.Fatalf("Failed to initialize GRPCTransport on %s: %v", listenAddr, err)
	}
	// Note: grpcTransport.Close() is called in gracefulShutdown
	log.Printf("Initialized GRPCTransport listening on %s", grpcTransport.LocalAddr())

	// Create Raft configuration
	raftConfig := raft.Config{
		ID:               cfg.ID,
		Peers:            cfg.Peers,
		ElectionTimeout:  defaultElectionTimeout,
		HeartbeatTimeout: defaultHeartbeatTimeout,
	}

	// Create RaftNode with all components
	raftNode, err := raft.NewRaft(raftConfig, boltStore, boltStore, kvStore, grpcTransport, snapshotStore)
	if err != nil {
		log.Fatalf("Failed to create RaftNode: %v", err)
	}
	log.Printf("Created RaftNode with ID=%s, Peers=%v", cfg.ID, cfg.Peers)

	// Start the Raft consensus loop
	raftNode.Start()
	log.Printf("Started Raft consensus loop")

	// Check if TUI mode is enabled
	if cfg.TUI {
		log.Printf("Starting TUI dashboard mode...")

		// Create RaftDataFetcher with Raft node, KVStore, and LogStore
		fetcher := tui.NewRaftDataFetcher(raftNode, kvStore, boltStore)

		// Create and run the TUI application
		tuiApp := tui.NewApp(fetcher)

		// Run TUI in a goroutine so we can handle shutdown
		tuiErrChan := make(chan error, 1)
		go func() {
			tuiErrChan <- tuiApp.Run()
		}()

		// Set up signal handling for graceful shutdown
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		// Wait for TUI to exit or signal
		select {
		case err := <-tuiErrChan:
			if err != nil {
				log.Printf("TUI error: %v", err)
			}
		case sig := <-sigChan:
			log.Printf("Received signal %v, stopping TUI...", sig)
			tuiApp.Stop()
			<-tuiErrChan // Wait for TUI to finish
		}

		// Graceful shutdown sequence (no HTTP server in TUI mode)
		exitCode := gracefulShutdownTUI(raftNode, grpcTransport, boltStore)
		os.Exit(exitCode)
	}

	// Set up HTTP server for KV API and status endpoint
	kvHandler := NewKVHandler(raftNode, kvStore)
	statusHandler := NewStatusHandler(raftNode, cfg.ID)
	mux := http.NewServeMux()
	mux.Handle("/kv/", kvHandler)
	mux.Handle("/status", statusHandler)

	httpAddr := fmt.Sprintf(":%d", cfg.HTTPPort)
	httpServer := &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	// Start HTTP server in a goroutine
	go func() {
		log.Printf("Starting HTTP server on %s", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Block until we receive a signal
	sig := <-sigChan
	log.Printf("Received signal %v, initiating graceful shutdown...", sig)

	// Graceful shutdown sequence
	exitCode := gracefulShutdown(httpServer, raftNode, grpcTransport, boltStore)
	os.Exit(exitCode)
}

// gracefulShutdownTUI performs an orderly shutdown for TUI mode (no HTTP server).
// It stops the Raft consensus loop, closes the gRPC transport, and closes the BoltStore.
// Returns 0 on successful shutdown, 1 on error.
func gracefulShutdownTUI(raftNode *raft.Raft, grpcTransport *transport.GRPCTransport, boltStore *storage.BoltStore) int {
	exitCode := 0

	// 1. Stop the Raft consensus loop
	log.Printf("Stopping Raft consensus loop...")
	if err := raftNode.Stop(); err != nil {
		log.Printf("Error stopping Raft: %v", err)
		exitCode = 1
	} else {
		log.Printf("Raft consensus loop stopped")
	}

	// 2. Close the GRPCTransport and release network resources
	log.Printf("Closing gRPC transport...")
	if err := grpcTransport.Close(); err != nil {
		log.Printf("Error closing gRPC transport: %v", err)
		exitCode = 1
	} else {
		log.Printf("gRPC transport closed")
	}

	// 3. Close the BoltStore and flush pending writes
	log.Printf("Closing BoltStore...")
	if err := boltStore.Close(); err != nil {
		log.Printf("Error closing BoltStore: %v", err)
		exitCode = 1
	} else {
		log.Printf("BoltStore closed")
	}
	if exitCode == 0 {
		log.Printf("Graceful shutdown completed successfully")
	} else {
		log.Printf("Graceful shutdown completed with errors")
	}

	return exitCode
}

// gracefulShutdown performs an orderly shutdown of all components.
// It stops accepting new HTTP requests, stops the Raft consensus loop,
// closes the gRPC transport, and closes the BoltStore.
// Returns 0 on successful shutdown, 1 on error.
func gracefulShutdown(httpServer *http.Server, raftNode *raft.Raft, grpcTransport *transport.GRPCTransport, boltStore *storage.BoltStore) int {
	exitCode := 0

	// 1. Stop accepting new HTTP requests
	log.Printf("Stopping HTTP server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("Error shutting down HTTP server: %v", err)
		exitCode = 1
	} else {
		log.Printf("HTTP server stopped")
	}

	// 2. Stop the Raft consensus loop
	log.Printf("Stopping Raft consensus loop...")
	if err := raftNode.Stop(); err != nil {
		log.Printf("Error stopping Raft: %v", err)
		exitCode = 1
	} else {
		log.Printf("Raft consensus loop stopped")
	}

	// 3. Close the GRPCTransport and release network resources
	log.Printf("Closing gRPC transport...")
	if err := grpcTransport.Close(); err != nil {
		log.Printf("Error closing gRPC transport: %v", err)
		exitCode = 1
	} else {
		log.Printf("gRPC transport closed")
	}

	// 4. Close the BoltStore and flush pending writes
	log.Printf("Closing BoltStore...")
	if err := boltStore.Close(); err != nil {
		log.Printf("Error closing BoltStore: %v", err)
		exitCode = 1
	} else {
		log.Printf("BoltStore closed")
	}
	if exitCode == 0 {
		log.Printf("Graceful shutdown completed successfully")
	} else {
		log.Printf("Graceful shutdown completed with errors")
	}

	return exitCode
}

// runClusterMode starts multiple Raft nodes using ClusterManager and displays the TUI.
// This function handles the entire lifecycle of cluster mode operation.
func runClusterMode(cfg *ServerConfig) {
	log.Printf("Starting cluster mode with %d nodes...", cfg.Cluster)

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory %s: %v", cfg.DataDir, err)
	}

	// Create ClusterManager
	clusterMgr := tui.NewClusterManager(cfg.Cluster, cfg.Port, cfg.HTTPPort, cfg.DataDir)

	// Start all nodes
	if err := clusterMgr.Start(); err != nil {
		// Log errors but continue if some nodes started
		log.Printf("Warning during cluster startup: %v", err)
		for _, errMsg := range clusterMgr.GetFailedNodeErrors() {
			log.Printf("  - %s", errMsg)
		}
	}

	// Check if any nodes started successfully
	if clusterMgr.SuccessfulNodeCount() == 0 {
		log.Fatalf("Failed to start any nodes in the cluster")
	}

	log.Printf("%s", clusterMgr.FormatStartupStatus())

	// Give nodes time to initialize
	time.Sleep(500 * time.Millisecond)

	// Create FetcherPool and add fetchers for all nodes
	fetcherPool := tui.NewFetcherPool()
	for _, node := range clusterMgr.GetNodes() {
		if node.Error == nil && node.Process != nil {
			// Create HTTP fetcher for each node
			fetcher := tui.NewHTTPDataFetcher(fmt.Sprintf("http://localhost:%d", node.HTTPPort), node.ID)
			fetcherPool.AddFetcher(node.ID, fetcher)
		}
	}

	// Create and run the TUI application with multi-node support
	tuiApp := tui.NewMultiNodeApp(cfg.Cluster, fetcherPool)

	// Run TUI in a goroutine so we can handle shutdown
	tuiErrChan := make(chan error, 1)
	go func() {
		tuiErrChan <- tuiApp.Run()
	}()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for TUI to exit or signal
	select {
	case err := <-tuiErrChan:
		if err != nil {
			log.Printf("TUI error: %v", err)
		}
	case sig := <-sigChan:
		log.Printf("Received signal %v, stopping TUI...", sig)
		tuiApp.Stop()
		<-tuiErrChan // Wait for TUI to finish
	}

	// Graceful shutdown - terminate all cluster nodes
	exitCode := gracefulShutdownCluster(clusterMgr, fetcherPool)
	os.Exit(exitCode)
}

// gracefulShutdownCluster performs an orderly shutdown of all cluster nodes.
// It stops all spawned node processes and closes the fetcher pool.
// Returns 0 on successful shutdown, 1 on error.
func gracefulShutdownCluster(clusterMgr *tui.ClusterManager, fetcherPool *tui.FetcherPool) int {
	exitCode := 0

	// 1. Close the FetcherPool connections
	log.Printf("Closing fetcher pool connections...")
	if err := fetcherPool.Close(); err != nil {
		log.Printf("Error closing fetcher pool: %v", err)
		exitCode = 1
	} else {
		log.Printf("Fetcher pool closed")
	}

	// 2. Stop all cluster node processes
	log.Printf("Stopping all cluster nodes...")
	if err := clusterMgr.Stop(); err != nil {
		log.Printf("Error stopping cluster nodes: %v", err)
		exitCode = 1
	} else {
		log.Printf("All cluster nodes stopped")
	}

	if exitCode == 0 {
		log.Printf("Cluster shutdown completed successfully")
	} else {
		log.Printf("Cluster shutdown completed with errors")
	}

	return exitCode
}
