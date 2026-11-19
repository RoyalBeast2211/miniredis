package main

import (
	"context"
	"fmt"
	"net"
	_ "net/http/pprof"
	"os"
	"strconv"

	"miniredis/aof"
	"miniredis/config"
	"miniredis/logger"
	"miniredis/protocol"
	"miniredis/replication"
	"miniredis/snapshot"
	"miniredis/storage"
)

// ServerContext adapts the unified Server struct for backwards compatibility with earlier test suites.
type ServerContext struct {
	server  *Server
	replica *replication.Replica
}

// NewServerContext creates a backwards-compatible ServerContext.
func NewServerContext(store *storage.Store, aofWriter *aof.AOF, isReplica bool, replicaOfAddr string, serverPort string) *ServerContext {
	cfg := config.NewDefaultConfig()
	if isReplica {
		cfg.ReplicaOf = replicaOfAddr
	}
	if p, err := strconv.Atoi(serverPort); err == nil && p > 0 {
		cfg.Port = p
	}

	s := NewServer(cfg, store, aofWriter)
	return &ServerContext{
		server:  s,
		replica: s.replica,
	}
}

// IsReplica returns whether the server is configured as a replica.
func (ctx *ServerContext) IsReplica() bool {
	return ctx.server.IsReplica()
}

// Close gracefully stops the server context.
func (ctx *ServerContext) Close() {
	_ = ctx.server.Shutdown(context.Background())
}

// handleCommand dispatches a command through the server's command dispatcher (backwards compatibility).
func handleCommand(ctx *ServerContext, args []string) protocol.Value {
	return ctx.server.dispatchCommand(args)
}

// handleConnection manages communication with one client (backwards compatibility).
func handleConnection(conn net.Conn, store *storage.Store, aofWriter *aof.AOF) {
	s := NewServer(nil, store, aofWriter)
	s.clientWg.Add(1)
	s.handleConnection(conn)
}

// handleConnectionWithContext manages communication with one client using a ServerContext.
func handleConnectionWithContext(conn net.Conn, ctx *ServerContext) {
	ctx.server.clientWg.Add(1)
	ctx.server.handleConnection(conn)
}

// runServer starts listening for TCP connections on the specified address with default Primary context.
func runServer(listener net.Listener, store *storage.Store, aofWriter *aof.AOF) error {
	s := NewServer(nil, store, aofWriter)
	return s.Serve(listener)
}

// runServerWithContext starts listening with the given ServerContext.
func runServerWithContext(listener net.Listener, ctx *ServerContext) error {
	return ctx.server.Serve(listener)
}

// main is the production entrypoint of the MiniRedis database server.
func main() {
	// Step 1: Load and validate configuration from file and/or command-line flags
	cfg, err := config.LoadFromFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		os.Exit(1)
	}

	// Step 2: Initialize structured logger
	logger.Init(cfg.LogLevel)

	logger.Info("Starting MiniRedis server...",
		"version", "1.0.0",
		"port", cfg.Port,
		"host", cfg.Host,
		"max_keys", cfg.MaxKeys,
	)

	// Step 3: Initialize in-memory storage engine
	var store *storage.Store
	if cfg.MaxKeys > 0 {
		store = storage.NewStoreWithCapacity(cfg.MaxKeys)
	} else {
		store = storage.NewStore()
	}

	// Step 4: Restore from snapshot if present
	var startAOFOffset int64 = 0
	header, snapEntries, err := snapshot.Load(cfg.SnapshotPath)
	if err == nil {
		store.LoadSnapshot(snapEntries)
		startAOFOffset = header.AOFOffset
		logger.Info("Database snapshot restored",
			"path", cfg.SnapshotPath,
			"keys_restored", header.KeyCount,
			"paired_aof_offset", header.AOFOffset,
		)
	} else if !os.IsNotExist(err) {
		logger.Error("Failed to load snapshot", "path", cfg.SnapshotPath, "error", err)
		os.Exit(1)
	}

	// Step 5: Replay AOF records occurring after the snapshot's AOF offset
	recoveryResult, err := aof.RecoverWithOffset(cfg.AOFPath, store, startAOFOffset)
	if err != nil {
		logger.Error("AOF recovery failed", "path", cfg.AOFPath, "error", err)
		os.Exit(1)
	}
	if recoveryResult.CommandsReplayed > 0 {
		logger.Info("AOF replay completed",
			"commands_replayed", recoveryResult.CommandsReplayed,
			"start_offset", startAOFOffset,
		)
	}
	if recoveryResult.TruncatedBytes > 0 {
		logger.Warn("AOF recovery truncated torn trailing bytes",
			"bytes_truncated", recoveryResult.TruncatedBytes,
		)
	}

	// Step 6: Open AOF for appending new mutations
	var syncPolicy aof.SyncPolicy
	switch cfg.AppendFsync {
	case "always":
		syncPolicy = aof.SyncAlways
	case "no":
		syncPolicy = aof.SyncNo
	default:
		syncPolicy = aof.SyncEverySecond
	}

	aofWriter, err := aof.Open(cfg.AOFPath, syncPolicy)
	if err != nil {
		logger.Error("Failed to open AOF file", "path", cfg.AOFPath, "error", err)
		os.Exit(1)
	}

	// Step 7: Construct server and start accepting connections
	server := NewServer(cfg, store, aofWriter)

	if err := server.Start(); err != nil {
		logger.Error("Server terminated unexpectedly", "error", err)
		os.Exit(1)
	}
}
