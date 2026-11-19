package replication

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"miniredis/protocol"
	"miniredis/snapshot"
	"miniredis/storage"
)

var (
	// ErrReplicaClosed indicates an operation was attempted on a stopped replica.
	ErrReplicaClosed = errors.New("replication: replica is closed")

	// ErrSyncTimeout is returned when waiting for synchronization exceeds the deadline.
	ErrSyncTimeout = errors.New("replication: sync timeout exceeded")
)

// Replica coordinates connecting to a primary, performing initial synchronization,
// consuming the incremental replication stream, and applying commands to local storage.
type Replica struct {
	primaryAddr   string
	listeningPort string
	store         *storage.Store

	mu            sync.RWMutex
	replID        string
	appliedOffset int64
	isConnected   bool

	closed   atomic.Bool
	closeCh  chan struct{}
	syncedCh chan struct{}
	syncOnce sync.Once

	// connMu protects outbound writes to the active primary TCP connection.
	connMu sync.Mutex
	conn   net.Conn

	// retryInterval is the duration to wait before attempting to reconnect to the primary.
	retryInterval time.Duration

	// ackInterval is the duration between REPLCONF ACK heartbeats.
	ackInterval time.Duration
}

// NewReplica initializes a new Replica instance with default reconnect and ACK intervals.
func NewReplica(primaryAddr string, listeningPort string, store *storage.Store) *Replica {
	return &Replica{
		primaryAddr:   primaryAddr,
		listeningPort: listeningPort,
		store:         store,
		closeCh:       make(chan struct{}),
		syncedCh:      make(chan struct{}),
		retryInterval: 100 * time.Millisecond,
		ackInterval:   200 * time.Millisecond,
	}
}

// SetIntervals allows configuring reconnect and heartbeat intervals (primarily for testing).
func (r *Replica) SetIntervals(retry time.Duration, ack time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if retry > 0 {
		r.retryInterval = retry
	}
	if ack > 0 {
		r.ackInterval = ack
	}
}

// Start begins the background replication connection and processing loop.
func (r *Replica) Start() {
	go r.runLoop()
}

// Close gracefully stops the replica and terminates connection to the primary.
func (r *Replica) Close() {
	if r.closed.Swap(true) {
		return
	}
	close(r.closeCh)

	r.connMu.Lock()
	if r.conn != nil {
		_ = r.conn.Close()
	}
	r.connMu.Unlock()
}

// ReplID returns the current replication timeline identifier.
func (r *Replica) ReplID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.replID
}

// AppliedOffset returns the cumulative byte offset applied by this replica.
func (r *Replica) AppliedOffset() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.appliedOffset
}

// IsConnected returns whether the replica currently holds an active connection to the primary.
func (r *Replica) IsConnected() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isConnected
}

// WaitForInitialSync blocks until the initial synchronization with the primary completes
// or the provided timeout expires.
func (r *Replica) WaitForInitialSync(timeout time.Duration) error {
	select {
	case <-r.syncedCh:
		return nil
	case <-r.closeCh:
		return ErrReplicaClosed
	case <-time.After(timeout):
		return ErrSyncTimeout
	}
}

// writeConn safely writes data to the primary connection under connMu.
func (r *Replica) writeConn(conn net.Conn, data []byte) error {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	_, err := conn.Write(data)
	return err
}

// runLoop maintains a persistent connection to the primary with automatic reconnection.
func (r *Replica) runLoop() {
	for !r.closed.Load() {
		conn, err := net.Dial("tcp", r.primaryAddr)
		if err != nil {
			select {
			case <-r.closeCh:
				return
			case <-time.After(r.retryInterval):
				continue
			}
		}

		r.connMu.Lock()
		r.conn = conn
		r.connMu.Unlock()

		r.mu.Lock()
		r.isConnected = true
		r.mu.Unlock()

		// Execute handshake and stream processing
		err = r.handlePrimarySession(conn)

		r.mu.Lock()
		r.isConnected = false
		r.mu.Unlock()

		_ = conn.Close()

		if r.closed.Load() {
			return
		}

		select {
		case <-r.closeCh:
			return
		case <-time.After(r.retryInterval):
		}
	}
}

// handlePrimarySession carries out the handshake and consumes the replication stream.
func (r *Replica) handlePrimarySession(conn net.Conn) error {
	reader := bufio.NewReader(conn)

	// Step 1: Handshake - Send PING
	if err := r.writeConn(conn, protocol.EncodeCommand("PING")); err != nil {
		return err
	}
	line, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	if strings.TrimSpace(line) != "+PONG" {
		return fmt.Errorf("expected +PONG, got %s", line)
	}

	// Step 2: Handshake - Send REPLCONF listening-port <port>
	port := r.listeningPort
	if port == "" {
		port = "6380"
	}
	if err := r.writeConn(conn, protocol.EncodeCommand("REPLCONF", "listening-port", port)); err != nil {
		return err
	}
	line, err = reader.ReadString('\n')
	if err != nil {
		return err
	}
	if strings.TrimSpace(line) != "+OK" {
		return fmt.Errorf("expected +OK from replconf, got %s", line)
	}

	// Step 3: Negotiate Sync with PSYNC <repl_id> <offset>
	r.mu.RLock()
	currentReplID := r.replID
	currentOffset := r.appliedOffset
	r.mu.RUnlock()

	psyncReplID := currentReplID
	psyncOffsetStr := strconv.FormatInt(currentOffset, 10)
	if psyncReplID == "" {
		psyncReplID = "?"
		psyncOffsetStr = "-1"
	}

	if err := r.writeConn(conn, protocol.EncodeCommand("PSYNC", psyncReplID, psyncOffsetStr)); err != nil {
		return err
	}

	line, err = reader.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimSpace(line)

	// Step 4: Handle PSYNC response
	protoReader := protocol.NewReader(reader)

	if strings.HasPrefix(line, "+CONTINUE") {
		// Partial Resynchronization granted
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			r.mu.Lock()
			r.replID = parts[1]
			r.mu.Unlock()
		}
		r.syncOnce.Do(func() { close(r.syncedCh) })
	} else if strings.HasPrefix(line, "+FULLRESYNC") {
		// Full Resynchronization: parse "+FULLRESYNC <repl_id> <offset>"
		parts := strings.Fields(line)
		if len(parts) < 3 {
			return fmt.Errorf("invalid fullresync response: %s", line)
		}
		newReplID := parts[1]
		newOffset, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid offset in fullresync: %w", err)
		}

		// Read bulk string containing the binary snapshot payload
		bulkVal, err := protoReader.ReadValue()
		if err != nil {
			return fmt.Errorf("failed to read snapshot payload: %w", err)
		}
		if bulkVal.Type != protocol.TypeBulkString {
			return fmt.Errorf("expected bulk string for snapshot payload, got %v", bulkVal.Type)
		}

		// Decode and install snapshot
		_, entries, err := snapshot.Decode([]byte(bulkVal.Str))
		if err != nil {
			return fmt.Errorf("failed to decode snapshot payload: %w", err)
		}

		r.store.LoadSnapshot(entries)

		r.mu.Lock()
		r.replID = newReplID
		r.appliedOffset = newOffset
		r.mu.Unlock()

		r.syncOnce.Do(func() { close(r.syncedCh) })
	} else {
		return fmt.Errorf("unexpected psync response: %s", line)
	}

	// Step 5: Start background heartbeat goroutine
	heartbeatDone := make(chan struct{})
	defer close(heartbeatDone)

	go func() {
		ticker := time.NewTicker(r.ackInterval)
		defer ticker.Stop()

		for {
			select {
			case <-heartbeatDone:
				return
			case <-r.closeCh:
				return
			case <-ticker.C:
				offset := r.AppliedOffset()
				ackCmd := protocol.EncodeCommand("REPLCONF", "ACK", strconv.FormatInt(offset, 10))
				if err := r.writeConn(conn, ackCmd); err != nil {
					return
				}
			}
		}
	}()

	// Step 6: Continuously consume incremental command stream
	for !r.closed.Load() {
		val, err := protoReader.ReadValue()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		args, err := val.ToCommand()
		if err != nil {
			continue
		}

		// Calculate exact byte length of the replicated command frame
		cmdByteLen := int64(len(protocol.EncodeCommand(args...)))

		// Apply mutation locally
		r.applyCommand(args)

		// Advance applied offset
		r.mu.Lock()
		r.appliedOffset += cmdByteLen
		r.mu.Unlock()
	}

	return nil
}

// applyCommand applies a state mutation received from the primary replication stream.
func (r *Replica) applyCommand(args []string) {
	if len(args) == 0 {
		return
	}

	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "SET":
		if len(args) == 3 {
			r.store.Set(args[1], args[2])
		}
	case "DEL":
		if len(args) == 2 {
			r.store.Delete(args[1])
		}
	case "EXPIREAT":
		if len(args) == 3 {
			if unixSec, err := strconv.ParseInt(args[2], 10, 64); err == nil {
				r.store.ExpireAt(args[1], unixSec)
			}
		}
	case "PERSIST":
		if len(args) == 2 {
			r.store.Persist(args[1])
		}
	case "PING":
		// Primary keepalive heartbeat; no storage mutation
	}
}
