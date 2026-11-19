package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"miniredis/aof"
	"miniredis/config"
	"miniredis/logger"
	"miniredis/protocol"
	"miniredis/replication"
	"miniredis/snapshot"
	"miniredis/stats"
	"miniredis/storage"
)

// takeSnapshot coordinates capturing a consistent point-in-time snapshot paired with the AOF offset.
func takeSnapshot(store *storage.Store, aofWriter *aof.AOF, path string) error {
	var aofOffset int64 = 0
	if aofWriter != nil {
		offset, err := aofWriter.Offset()
		if err != nil {
			return fmt.Errorf("failed to get aof offset: %w", err)
		}
		aofOffset = offset
	}

	entries := store.SnapshotData()
	return snapshot.Save(path, aofOffset, entries)
}

// Server coordinates all subsystem components of MiniRedis:
// networking, command dispatch, storage, persistence, replication, and telemetry.
type Server struct {
	cfg       *config.Config
	store     *storage.Store
	aofWriter *aof.AOF
	primary   *replication.Primary
	replica   *replication.Replica
	metrics   *stats.Metrics

	// Replication state
	mu          sync.RWMutex
	isReplica   bool
	primaryHost string
	primaryPort string

	// Network listener
	listener     net.Listener
	healthServer *http.Server
	pprofServer  *http.Server

	// Active connection management
	connsMu  sync.Mutex
	conns    map[net.Conn]struct{}
	clientWg sync.WaitGroup

	// Lifecycle flags
	isReady      atomic.Bool
	isShutting   atomic.Bool
	stopCh       chan struct{}
	shutdownOnce sync.Once
}

// NewServer initializes a configured MiniRedis Server instance.
func NewServer(cfg *config.Config, store *storage.Store, aofWriter *aof.AOF) *Server {
	if cfg == nil {
		cfg = config.NewDefaultConfig()
	}

	s := &Server{
		cfg:       cfg,
		store:     store,
		aofWriter: aofWriter,
		metrics:   stats.NewMetrics(),
		conns:     make(map[net.Conn]struct{}),
		stopCh:    make(chan struct{}),
	}

	// Initialize replication role
	if cfg.ReplicaOf != "" {
		host, port, err := net.SplitHostPort(cfg.ReplicaOf)
		if err != nil {
			parts := strings.Fields(cfg.ReplicaOf)
			if len(parts) == 2 {
				host, port = parts[0], parts[1]
			} else {
				host, port = "127.0.0.1", cfg.ReplicaOf
			}
		}
		s.isReplica = true
		s.primaryHost = host
		s.primaryPort = port
		s.replica = replication.NewReplica(net.JoinHostPort(host, port), strconv.Itoa(cfg.Port), store)
		s.replica.Start()
	} else {
		s.isReplica = false
		s.primary = replication.NewPrimary(store, cfg.ReplBacklogSize)
	}

	return s
}

// IsReplica returns whether the server is currently configured as a read-only replica.
func (s *Server) IsReplica() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isReplica
}

// Metrics returns the server's telemetry metrics tracker.
func (s *Server) Metrics() *stats.Metrics {
	return s.metrics
}

// Start begins listening on TCP and blocks until the server shuts down or encounters a fatal error.
func (s *Server) Start() error {
	address := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to bind TCP listener on %s: %w", address, err)
	}
	s.listener = listener

	// Start optional HTTP health server
	if s.cfg.HealthPort > 0 {
		s.startHealthServer()
	}

	// Start optional HTTP pprof server
	if s.cfg.PprofPort > 0 {
		s.startPprofServer()
	}

	// Set ready state
	s.isReady.Store(true)

	role := "primary"
	if s.IsReplica() {
		role = fmt.Sprintf("replica of %s:%s", s.primaryHost, s.primaryPort)
	}
	logger.Info("MiniRedis ready to accept connections",
		"address", address,
		"role", role,
		"aof", s.cfg.AOFPath,
		"snapshot", s.cfg.SnapshotPath,
		"fsync", s.cfg.AppendFsync,
	)

	// Intercept SIGINT and SIGTERM for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info("Received shutdown signal", "signal", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			logger.Error("Error during shutdown", "error", err)
		}
	}()

	return s.Serve(listener)
}

// Serve accepts client connections on the provided listener until closed.
func (s *Server) Serve(listener net.Listener) error {
	s.listener = listener
	for {
		conn, err := listener.Accept()
		if err != nil {
			if s.isShutting.Load() {
				return nil
			}
			return err
		}

		// Enforce MaxClients limit
		if s.metrics.ActiveConnections.Load() >= int64(s.cfg.MaxClients) {
			s.metrics.ConnRejected()
			logger.Warn("Rejected client: maxclients limit reached",
				"remote_addr", conn.RemoteAddr().String(),
				"max_clients", s.cfg.MaxClients,
			)
			errResp := protocol.NewError("ERR max number of clients reached")
			_, _ = conn.Write(errResp.Encode())
			_ = conn.Close()
			continue
		}

		s.clientWg.Add(1)
		s.connsMu.Lock()
		s.conns[conn] = struct{}{}
		s.connsMu.Unlock()

		go s.handleConnection(conn)
	}
}

// handleConnection manages communication with a single client connection.
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		s.connsMu.Lock()
		delete(s.conns, conn)
		s.connsMu.Unlock()
		s.clientWg.Done()
		_ = conn.Close()
	}()

	s.metrics.ConnAccepted()
	defer s.metrics.ConnClosed()

	clientAddr := conn.RemoteAddr().String()
	reader := protocol.NewReader(conn)
	var activePeer *replication.ReplicaPeer

	defer func() {
		if activePeer != nil {
			s.mu.RLock()
			if s.primary != nil {
				s.primary.RemovePeer(activePeer.ID())
			}
			s.mu.RUnlock()
			activePeer.Close()
		}
	}()

	for {
		if s.isShutting.Load() {
			return
		}

		// Apply idle timeout if configured
		if s.cfg.ClientTimeout > 0 {
			_ = conn.SetDeadline(time.Now().Add(s.cfg.ClientTimeout))
		}

		val, err := reader.ReadValue()
		if err != nil {
			return
		}

		args, err := val.ToCommand()
		if err != nil {
			errResp := protocol.NewError(fmt.Sprintf("ERR %v", err))
			if _, writeErr := conn.Write(errResp.Encode()); writeErr != nil {
				return
			}
			continue
		}
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])

		// Handle replication handshake and synchronization frames
		if cmd == "REPLCONF" {
			s.mu.RLock()
			primary := s.primary
			s.mu.RUnlock()

			if primary != nil {
				resp := primary.HandleReplConf(clientAddr, args)
				if resp.Type != 0 {
					if _, err := conn.Write(resp.Encode()); err != nil {
						return
					}
				}
				continue
			}
		}

		if cmd == "PSYNC" {
			s.mu.RLock()
			primary := s.primary
			s.mu.RUnlock()

			if primary != nil && len(args) >= 3 {
				reqReplID := args[1]
				reqOffset, _ := strconv.ParseInt(args[2], 10, 64)
				peer, err := primary.HandlePSync(conn, reqReplID, reqOffset)
				if err != nil {
					return
				}
				activePeer = peer
				continue
			}
		}

		// Dispatch standard command
		s.metrics.IncrCommands()
		response := s.dispatchCommand(args)

		if _, writeErr := conn.Write(response.Encode()); writeErr != nil {
			return
		}
	}
}

// dispatchCommand executes a parsed command against storage, persistence, and replication.
func (s *Server) dispatchCommand(args []string) protocol.Value {
	cmd := strings.ToUpper(args[0])

	// Read-Only Enforcement for Replicas
	if s.IsReplica() {
		switch cmd {
		case "SET", "DEL", "EXPIRE", "EXPIREAT", "PERSIST":
			return protocol.NewError("READONLY You can't write against a read only replica.")
		}
	}

	switch cmd {
	case "PING":
		s.metrics.IncrReads()
		if len(args) == 1 {
			return protocol.NewSimpleString("PONG")
		} else if len(args) == 2 {
			return protocol.NewBulkString(args[1])
		}
		return protocol.NewError("ERR wrong number of arguments for 'ping' command")

	case "ECHO":
		s.metrics.IncrReads()
		if len(args) != 2 {
			return protocol.NewError("ERR wrong number of arguments for 'echo' command")
		}
		return protocol.NewBulkString(args[1])

	case "SET":
		if len(args) != 3 {
			return protocol.NewError("ERR wrong number of arguments for 'set' command")
		}
		s.metrics.IncrWrites()
		s.store.Set(args[1], args[2])

		if s.aofWriter != nil {
			if err := s.aofWriter.Write([]string{"SET", args[1], args[2]}); err != nil {
				return protocol.NewError("ERR aof write failed: " + err.Error())
			}
		}

		s.mu.RLock()
		if s.primary != nil {
			s.primary.Propagate([]string{"SET", args[1], args[2]})
		}
		s.mu.RUnlock()

		return protocol.NewSimpleString("OK")

	case "GET":
		s.metrics.IncrReads()
		if len(args) != 2 {
			return protocol.NewError("ERR wrong number of arguments for 'get' command")
		}
		val, exists := s.store.Get(args[1])
		if !exists {
			return protocol.NewNullBulkString()
		}
		return protocol.NewBulkString(val)

	case "DEL":
		if len(args) != 2 {
			return protocol.NewError("ERR wrong number of arguments for 'del' command")
		}
		s.metrics.IncrWrites()
		deleted := s.store.Delete(args[1])
		if deleted {
			if s.aofWriter != nil {
				if err := s.aofWriter.Write([]string{"DEL", args[1]}); err != nil {
					return protocol.NewError("ERR aof write failed: " + err.Error())
				}
			}
			s.mu.RLock()
			if s.primary != nil {
				s.primary.Propagate([]string{"DEL", args[1]})
			}
			s.mu.RUnlock()
			return protocol.NewInteger(1)
		}
		return protocol.NewInteger(0)

	case "EXISTS":
		s.metrics.IncrReads()
		if len(args) != 2 {
			return protocol.NewError("ERR wrong number of arguments for 'exists' command")
		}
		if s.store.Exists(args[1]) {
			return protocol.NewInteger(1)
		}
		return protocol.NewInteger(0)

	case "EXPIRE":
		if len(args) != 3 {
			return protocol.NewError("ERR wrong number of arguments for 'expire' command")
		}
		seconds, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return protocol.NewError("ERR value is not an integer or out of range")
		}

		s.metrics.IncrWrites()
		deadlineUnix := time.Now().Add(time.Duration(seconds) * time.Second).Unix()

		if ok := s.store.Expire(args[1], seconds); ok {
			if s.aofWriter != nil {
				var aofErr error
				if seconds <= 0 {
					aofErr = s.aofWriter.Write([]string{"DEL", args[1]})
				} else {
					aofErr = s.aofWriter.Write([]string{"EXPIREAT", args[1], strconv.FormatInt(deadlineUnix, 10)})
				}
				if aofErr != nil {
					return protocol.NewError("ERR aof write failed: " + aofErr.Error())
				}
			}

			s.mu.RLock()
			if s.primary != nil {
				if seconds <= 0 {
					s.primary.Propagate([]string{"DEL", args[1]})
				} else {
					s.primary.Propagate([]string{"EXPIREAT", args[1], strconv.FormatInt(deadlineUnix, 10)})
				}
			}
			s.mu.RUnlock()

			return protocol.NewInteger(1)
		}
		return protocol.NewInteger(0)

	case "EXPIREAT":
		if len(args) != 3 {
			return protocol.NewError("ERR wrong number of arguments for 'expireat' command")
		}
		unixSec, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return protocol.NewError("ERR value is not an integer or out of range")
		}
		s.metrics.IncrWrites()
		if ok := s.store.ExpireAt(args[1], unixSec); ok {
			if s.aofWriter != nil {
				if err := s.aofWriter.Write([]string{"EXPIREAT", args[1], args[2]}); err != nil {
					return protocol.NewError("ERR aof write failed: " + err.Error())
				}
			}
			s.mu.RLock()
			if s.primary != nil {
				s.primary.Propagate([]string{"EXPIREAT", args[1], args[2]})
			}
			s.mu.RUnlock()
			return protocol.NewInteger(1)
		}
		return protocol.NewInteger(0)

	case "TTL":
		s.metrics.IncrReads()
		if len(args) != 2 {
			return protocol.NewError("ERR wrong number of arguments for 'ttl' command")
		}
		ttl := s.store.TTL(args[1])
		return protocol.NewInteger(ttl)

	case "PERSIST":
		if len(args) != 2 {
			return protocol.NewError("ERR wrong number of arguments for 'persist' command")
		}
		s.metrics.IncrWrites()
		if ok := s.store.Persist(args[1]); ok {
			if s.aofWriter != nil {
				if err := s.aofWriter.Write([]string{"PERSIST", args[1]}); err != nil {
					return protocol.NewError("ERR aof write failed: " + err.Error())
				}
			}
			s.mu.RLock()
			if s.primary != nil {
				s.primary.Propagate([]string{"PERSIST", args[1]})
			}
			s.mu.RUnlock()
			return protocol.NewInteger(1)
		}
		return protocol.NewInteger(0)

	case "SAVE":
		if len(args) != 1 {
			return protocol.NewError("ERR wrong number of arguments for 'save' command")
		}
		if err := takeSnapshot(s.store, s.aofWriter, s.cfg.SnapshotPath); err != nil {
			return protocol.NewError("ERR snapshot failed: " + err.Error())
		}
		return protocol.NewSimpleString("OK")

	case "BGREWRITEAOF":
		if len(args) != 1 {
			return protocol.NewError("ERR wrong number of arguments for 'bgrewriteaof' command")
		}
		if s.aofWriter == nil {
			return protocol.NewError("ERR AOF is not enabled")
		}
		if _, err := s.aofWriter.Rewrite(s.store, s.cfg.SnapshotPath); err != nil {
			return protocol.NewError("ERR aof rewrite failed: " + err.Error())
		}
		return protocol.NewSimpleString("OK")

	case "ROLE":
		return s.roleCommand()

	case "INFO":
		return s.infoCommand(args)

	case "REPLICAOF", "SLAVEOF":
		return s.replicaOfCommand(args)

	default:
		return protocol.NewError(fmt.Sprintf("ERR unknown command '%s'", args[0]))
	}
}

// roleCommand implements the standard Redis ROLE command.
func (s *Server) roleCommand() protocol.Value {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.isReplica {
		var offset int64 = 0
		if s.primary != nil {
			offset = s.primary.CurrentOffset()
		}
		return protocol.NewArray([]protocol.Value{
			protocol.NewBulkString("master"),
			protocol.NewInteger(offset),
			protocol.NewArray(nil),
		})
	}

	state := "disconnected"
	if s.replica != nil && s.replica.IsConnected() {
		state = "connected"
	}
	var offset int64 = 0
	if s.replica != nil {
		offset = s.replica.AppliedOffset()
	}
	portNum, _ := strconv.ParseInt(s.primaryPort, 10, 64)

	return protocol.NewArray([]protocol.Value{
		protocol.NewBulkString("slave"),
		protocol.NewBulkString(s.primaryHost),
		protocol.NewInteger(portNum),
		protocol.NewBulkString(state),
		protocol.NewInteger(offset),
	})
}

// infoCommand formats telemetry and subsystem states for the INFO command.
func (s *Server) infoCommand(args []string) protocol.Value {
	section := ""
	if len(args) > 1 {
		section = args[1]
	}

	s.mu.RLock()
	isRepl := s.isReplica
	primary := s.primary
	replica := s.replica
	primaryHost := s.primaryHost
	primaryPort := s.primaryPort
	s.mu.RUnlock()

	extra := make(map[string]string)

	// Persistence section
	var aofStatus string
	if s.aofWriter != nil {
		aofStatus = fmt.Sprintf("aof_enabled:1\r\naof_fsync_policy:%s\r\n", s.cfg.AppendFsync)
	} else {
		aofStatus = "aof_enabled:0\r\n"
	}
	extra["Persistence"] = aofStatus

	// Replication section
	var replBuilder strings.Builder
	if !isRepl {
		replBuilder.WriteString("role:master\r\n")
		count := 0
		var offset int64 = 0
		replID := "0000000000000000000000000000000000000000"
		if primary != nil {
			count = primary.ReplicaCount()
			offset = primary.CurrentOffset()
			replID = primary.ReplID()
		}
		replBuilder.WriteString(fmt.Sprintf("connected_slaves:%d\r\n", count))
		replBuilder.WriteString(fmt.Sprintf("master_replid:%s\r\n", replID))
		replBuilder.WriteString(fmt.Sprintf("master_repl_offset:%d\r\n", offset))
	} else {
		replBuilder.WriteString("role:slave\r\n")
		linkStatus := "down"
		if replica != nil && replica.IsConnected() {
			linkStatus = "up"
		}
		var offset int64 = 0
		if replica != nil {
			offset = replica.AppliedOffset()
		}
		replBuilder.WriteString(fmt.Sprintf("master_host:%s\r\n", primaryHost))
		replBuilder.WriteString(fmt.Sprintf("master_port:%s\r\n", primaryPort))
		replBuilder.WriteString(fmt.Sprintf("master_link_status:%s\r\n", linkStatus))
		replBuilder.WriteString(fmt.Sprintf("slave_repl_offset:%d\r\n", offset))
	}
	extra["Replication"] = replBuilder.String()

	infoText := s.metrics.FormatInfo(section, extra)
	return protocol.NewBulkString(infoText)
}

// replicaOfCommand implements dynamic failover and replication reconfiguration.
func (s *Server) replicaOfCommand(args []string) protocol.Value {
	if len(args) != 3 {
		return protocol.NewError("ERR wrong number of arguments for 'replicaof' command")
	}

	host := args[1]
	port := args[2]

	s.mu.Lock()
	defer s.mu.Unlock()

	if strings.ToUpper(host) == "NO" && strings.ToUpper(port) == "ONE" {
		if s.isReplica {
			if s.replica != nil {
				s.replica.Close()
				s.replica = nil
			}
			s.isReplica = false
			s.primary = replication.NewPrimary(s.store, s.cfg.ReplBacklogSize)
			logger.Info("Promoted replica to independent primary via REPLICAOF NO ONE")
		}
		return protocol.NewSimpleString("OK")
	}

	if s.replica != nil {
		s.replica.Close()
	}
	if s.primary != nil {
		s.primary.Close()
		s.primary = nil
	}

	s.isReplica = true
	s.primaryHost = host
	s.primaryPort = port
	s.replica = replication.NewReplica(net.JoinHostPort(host, port), strconv.Itoa(s.cfg.Port), s.store)
	s.replica.Start()

	logger.Info("Configured as replica of upstream primary", "target", net.JoinHostPort(host, port))
	return protocol.NewSimpleString("OK")
}

// startHealthServer mounts HTTP liveness and readiness probe endpoints.
func (s *Server) startHealthServer() {
	mux := http.NewServeMux()

	// /healthz returns 200 OK as long as the process is alive (liveness probe)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "alive",
			"uptime": int64(time.Since(s.metrics.StartTime).Seconds()),
		})
	})

	// /readyz returns 200 OK only when ready to accept queries (readiness probe)
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if s.isReady.Load() && !s.isShutting.Load() {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "ready",
				"role":   s.roleString(),
			})
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "not_ready",
			})
		}
	})

	healthAddr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.HealthPort)
	s.healthServer = &http.Server{
		Addr:    healthAddr,
		Handler: mux,
	}

	go func() {
		logger.Info("HTTP health check server listening", "address", fmt.Sprintf("http://%s/healthz", healthAddr))
		if err := s.healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Health server error", "error", err)
		}
	}()
}

// roleString returns a human-readable role identifier.
func (s *Server) roleString() string {
	if s.IsReplica() {
		return "replica"
	}
	return "primary"
}

// startPprofServer mounts the standard pprof profiling endpoints.
func (s *Server) startPprofServer() {
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)

	pprofAddr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.PprofPort)
	s.pprofServer = &http.Server{
		Addr:    pprofAddr,
		Handler: nil, // uses default http.DefaultServeMux where pprof registered
	}

	go func() {
		logger.Info("HTTP pprof profiling server listening", "address", fmt.Sprintf("http://%s/debug/pprof/", pprofAddr))
		if err := s.pprofServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("pprof server error", "error", err)
		}
	}()
}

// Shutdown cleanly coordinates the 5-stage graceful shutdown sequence:
// 1. Mark server not-ready (immediately failing /readyz probes).
// 2. Close TCP listener (rejecting new connections).
// 3. Wake up and drain active client connections within ctx timeout.
// 4. Terminate replication peer pumps and client loops.
// 5. Commit remaining AOF buffers and close files.
func (s *Server) Shutdown(ctx context.Context) error {
	var shutdownErr error

	s.shutdownOnce.Do(func() {
		logger.Info("Initiating graceful shutdown sequence...")
		s.isShutting.Store(true)
		s.isReady.Store(false)

		// Step 1: Close TCP listener to reject new connections
		if s.listener != nil {
			_ = s.listener.Close()
		}

		// Step 2: Shutdown HTTP health and pprof servers
		if s.healthServer != nil {
			_ = s.healthServer.Shutdown(ctx)
		}
		if s.pprofServer != nil {
			_ = s.pprofServer.Shutdown(ctx)
		}

		// Step 3: Notify active connections and drain with deadline
		close(s.stopCh)

		// Set short read deadlines on active connections to wake up idle reads
		s.connsMu.Lock()
		for conn := range s.conns {
			_ = conn.SetDeadline(time.Now().Add(100 * time.Millisecond))
		}
		s.connsMu.Unlock()

		// Wait for existing clients to finish or context to expire
		done := make(chan struct{})
		go func() {
			s.clientWg.Wait()
			close(done)
		}()

		select {
		case <-done:
			logger.Info("All active client connections cleanly drained")
		case <-ctx.Done():
			logger.Warn("Shutdown deadline exceeded; terminating remaining active connections", "timeout", s.cfg.ShutdownTimeout)
			s.connsMu.Lock()
			for conn := range s.conns {
				_ = conn.Close()
			}
			s.connsMu.Unlock()
		}

		// Step 4: Close replication subsystem
		s.mu.Lock()
		if s.primary != nil {
			s.primary.Close()
		}
		if s.replica != nil {
			s.replica.Close()
		}
		s.mu.Unlock()

		// Step 5: Stop storage active TTL cleaner
		s.store.Close()

		// Step 6: Flush and close AOF persistence
		if s.aofWriter != nil {
			if err := s.aofWriter.Close(); err != nil {
				logger.Error("Error closing AOF during shutdown", "error", err)
				shutdownErr = err
			} else {
				logger.Info("AOF persistence buffer flushed and closed cleanly")
			}
		}

		logger.Info("MiniRedis graceful shutdown complete. Goodbye!")
	})

	return shutdownErr
}
