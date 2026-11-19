package replication

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"miniredis/protocol"
	"miniredis/snapshot"
	"miniredis/storage"
)

var (
	// ErrPrimaryClosed is returned when an operation is attempted on a closed Primary instance.
	ErrPrimaryClosed = errors.New("replication: primary is closed")
)

// ReplicaPeer represents a connected follower replica receiving the replication stream.
type ReplicaPeer struct {
	id            string
	conn          net.Conn
	listeningPort string
	sendCh        chan []byte
	ackOffset     int64
	lastAckTime   time.Time
	mu            sync.Mutex
	closed        bool
	closeCh       chan struct{}
}

// newReplicaPeer initializes a new peer instance.
func newReplicaPeer(conn net.Conn) *ReplicaPeer {
	return &ReplicaPeer{
		id:          conn.RemoteAddr().String(),
		conn:        conn,
		sendCh:      make(chan []byte, 1024),
		lastAckTime: time.Now(),
		closeCh:     make(chan struct{}),
	}
}

// ID returns the unique identifier (remote address) of the replica peer.
func (peer *ReplicaPeer) ID() string {
	return peer.id
}

// Send enqueues data to be sent to the replica over its TCP socket.
// If the send buffer is full (slow consumer), the peer is dropped to protect the primary.
func (peer *ReplicaPeer) Send(data []byte) {
	peer.mu.Lock()
	if peer.closed {
		peer.mu.Unlock()
		return
	}
	peer.mu.Unlock()

	select {
	case peer.sendCh <- data:
	default:
		// Replica buffer is full; close peer asynchronously to avoid blocking the primary
		go peer.Close()
	}
}

// Close shuts down the replica peer connection and terminates its write pump.
func (peer *ReplicaPeer) Close() {
	peer.mu.Lock()
	if peer.closed {
		peer.mu.Unlock()
		return
	}
	peer.closed = true
	close(peer.closeCh)
	peer.mu.Unlock()

	_ = peer.conn.Close()
}

// writePump continuously reads outgoing replication packets from sendCh and writes them to the TCP socket.
func (peer *ReplicaPeer) writePump() {
	for {
		select {
		case <-peer.closeCh:
			return
		case data, ok := <-peer.sendCh:
			if !ok {
				return
			}
			if _, err := peer.conn.Write(data); err != nil {
				peer.Close()
				return
			}
		}
	}
}

// Primary coordinates replication to all connected replica nodes.
// It tracks replication offsets, maintains the circular backlog, and manages replica peers.
type Primary struct {
	mu      sync.RWMutex
	store   *storage.Store
	backlog *Backlog
	peers   map[string]*ReplicaPeer
	closed  bool
	closeCh chan struct{}
}

// NewPrimary creates a new Primary replication manager.
func NewPrimary(store *storage.Store, backlogCapacity int64) *Primary {
	return &Primary{
		store:   store,
		backlog: NewBacklog(backlogCapacity),
		peers:   make(map[string]*ReplicaPeer),
		closeCh: make(chan struct{}),
	}
}

// ReplID returns the primary's unique 40-character replication timeline identifier.
func (p *Primary) ReplID() string {
	return p.backlog.ReplID()
}

// CurrentOffset returns the primary's latest cumulative byte offset.
func (p *Primary) CurrentOffset() int64 {
	return p.backlog.CurrentOffset()
}

// ReplicaCount returns the number of currently active connected replicas.
func (p *Primary) ReplicaCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.peers)
}

// Backlog returns the underlying circular backlog ring buffer.
func (p *Primary) Backlog() *Backlog {
	return p.backlog
}

// Close gracefully closes all connected replica peers and halts replication operations.
func (p *Primary) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.closeCh)

	// Disconnect all connected replicas
	for _, peer := range p.peers {
		peer.Close()
	}
	p.peers = make(map[string]*ReplicaPeer)
	p.mu.Unlock()
}

// RemovePeer removes a replica peer from the active peer list.
func (p *Primary) RemovePeer(id string) {
	p.mu.Lock()
	delete(p.peers, id)
	p.mu.Unlock()
}

// Propagate serializes a state-changing command into RESP format,
// writes it into the replication backlog, and asynchronously broadcasts it to all connected replicas.
// Returns the new cumulative replication offset.
func (p *Primary) Propagate(args []string) int64 {
	encoded := protocol.EncodeCommand(args...)

	offset := p.backlog.Write(encoded)

	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, peer := range p.peers {
		peer.Send(encoded)
	}

	return offset
}

// HandleReplConf processes a REPLCONF command sent by a replica.
// Supported subcommands:
// - REPLCONF listening-port <port>
// - REPLCONF ACK <offset>
func (p *Primary) HandleReplConf(peerAddr string, args []string) protocol.Value {
	if len(args) < 3 {
		return protocol.NewError("ERR wrong number of arguments for 'replconf' command")
	}

	subCmd := strings.ToUpper(args[1])
	switch subCmd {
	case "LISTENING-PORT":
		p.mu.Lock()
		if peer, exists := p.peers[peerAddr]; exists {
			peer.listeningPort = args[2]
		}
		p.mu.Unlock()
		return protocol.NewSimpleString("OK")

	case "ACK":
		offset, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return protocol.NewError("ERR invalid offset in replconf ack")
		}

		p.mu.Lock()
		if peer, exists := p.peers[peerAddr]; exists {
			peer.mu.Lock()
			peer.ackOffset = offset
			peer.lastAckTime = time.Now()
			peer.mu.Unlock()
		}
		p.mu.Unlock()
		// REPLCONF ACK does not require a reply
		return protocol.Value{}

	default:
		return protocol.NewError(fmt.Sprintf("ERR unrecognized replconf option '%s'", subCmd))
	}
}

// HandlePSync handles the PSYNC negotiation with a replica.
// If the replica's timeline ID matches and the requested offset is within the backlog window,
// a Partial Resynchronization (+CONTINUE) is granted.
// Otherwise, a Full Resynchronization (+FULLRESYNC) is initiated via snapshot bulk transfer.
//
// Once PSYNC succeeds, this method returns a newly registered *ReplicaPeer, and starts its write pump.
func (p *Primary) HandlePSync(conn net.Conn, reqReplID string, reqOffset int64) (*ReplicaPeer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrPrimaryClosed
	}

	peer := newReplicaPeer(conn)

	// Check if Partial Resynchronization (PSYNC) is possible
	if p.backlog.CanPartialResync(reqReplID, reqOffset) {
		// 1. Send +CONTINUE <replID>\r\n
		continueMsg := fmt.Sprintf("+CONTINUE %s\r\n", p.ReplID())
		if _, err := conn.Write([]byte(continueMsg)); err != nil {
			return nil, err
		}

		// 2. Retrieve missed command bytes from backlog
		deltaBytes, err := p.backlog.GetSlice(reqReplID, reqOffset)
		if err != nil {
			return nil, err
		}

		// 3. Send backlog delta
		if len(deltaBytes) > 0 {
			if _, err := conn.Write(deltaBytes); err != nil {
				return nil, err
			}
		}

		peer.ackOffset = p.backlog.CurrentOffset()
		p.peers[peer.id] = peer
		go peer.writePump()
		return peer, nil
	}

	// Full Resynchronization (+FULLRESYNC) required
	// 1. Capture point-in-time snapshot and snapshot offset
	snapEntries := p.store.SnapshotData()
	snapOffset := p.backlog.CurrentOffset()

	snapBytes, err := snapshot.Encode(snapOffset, snapEntries)
	if err != nil {
		return nil, fmt.Errorf("failed to encode snapshot: %w", err)
	}

	// 2. Send +FULLRESYNC <replID> <snapOffset>\r\n
	fullResyncHeader := fmt.Sprintf("+FULLRESYNC %s %d\r\n", p.ReplID(), snapOffset)
	if _, err := conn.Write([]byte(fullResyncHeader)); err != nil {
		return nil, err
	}

	// 3. Send binary snapshot payload as RESP Bulk String: $<len>\r\n<bytes>\r\n
	bulkPayload := protocol.NewBulkString(string(snapBytes)).Encode()
	if _, err := conn.Write(bulkPayload); err != nil {
		return nil, err
	}

	// 4. Send any commands that arrived during snapshot generation
	currentOffset := p.backlog.CurrentOffset()
	if currentOffset > snapOffset {
		deltaBytes, err := p.backlog.GetSlice(p.ReplID(), snapOffset)
		if err == nil && len(deltaBytes) > 0 {
			if _, err := conn.Write(deltaBytes); err != nil {
				return nil, err
			}
		}
	}

	peer.ackOffset = currentOffset
	p.peers[peer.id] = peer
	go peer.writePump()
	return peer, nil
}

// GetReplicaAckOffset returns the latest acknowledged offset reported by a replica peer.
func (p *Primary) GetReplicaAckOffset(peerAddr string) (int64, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	peer, exists := p.peers[peerAddr]
	if !exists {
		return 0, false
	}

	peer.mu.Lock()
	defer peer.mu.Unlock()
	return peer.ackOffset, true
}
