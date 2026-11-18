package replication

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrOffsetOutOfRange is returned when a requested offset is older than the oldest retained byte
	// or newer than the latest byte written.
	ErrOffsetOutOfRange = errors.New("replication: requested offset is out of range")

	// ErrReplIDMismatch is returned when a replica attempts PSYNC with a replication ID
	// that does not match the primary's current timeline.
	ErrReplIDMismatch = errors.New("replication: replication ID mismatch")
)

// DefaultBacklogSize is the default capacity of the in-memory circular backlog (1 MB).
const DefaultBacklogSize = 1024 * 1024

// generateReplicationID generates a pseudo-random 40-character hexadecimal string
// representing the primary's replication timeline identity (identical in format to standard Redis).
func generateReplicationID() string {
	bytes := make([]byte, 20)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback in the rare case crypto/rand fails
		return "0000000000000000000000000000000000000000"
	}
	return hex.EncodeToString(bytes)
}

// Backlog is a thread-safe, fixed-size circular ring buffer that retains the most recent
// byte stream of state-changing RESP commands sent by the primary.
//
// Replicas that temporarily disconnect can perform a Partial Resynchronization (PSYNC)
// by requesting the bytes they missed since their last acknowledged offset.
type Backlog struct {
	mu sync.RWMutex

	// replID is the unique 40-character ID representing this primary's current timeline.
	replID string

	// buffer is the circular byte storage of fixed size capacity.
	buffer []byte

	// capacity is the fixed size in bytes of the circular buffer.
	capacity int64

	// firstOffset is the global byte offset of the oldest byte still retained in the buffer.
	// If no writes have occurred yet, firstOffset is 0.
	firstOffset int64

	// currentOffset is the global byte offset of the next byte to be written.
	// (Equal to total bytes ever written to this backlog).
	currentOffset int64
}

// NewBacklog constructs an initialized circular backlog with the specified capacity.
// If capacity is <= 0, DefaultBacklogSize (1 MB) is used.
func NewBacklog(capacity int64) *Backlog {
	if capacity <= 0 {
		capacity = DefaultBacklogSize
	}
	return &Backlog{
		replID:        generateReplicationID(),
		buffer:        make([]byte, capacity),
		capacity:      capacity,
		firstOffset:   0,
		currentOffset: 0,
	}
}

// ReplID returns the primary's unique 40-character replication timeline identifier.
func (b *Backlog) ReplID() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.replID
}

// CurrentOffset returns the latest cumulative byte offset written to this backlog.
func (b *Backlog) CurrentOffset() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.currentOffset
}

// FirstOffset returns the oldest cumulative byte offset still retained in this backlog.
func (b *Backlog) FirstOffset() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.firstOffset
}

// Capacity returns the maximum buffer size in bytes.
func (b *Backlog) Capacity() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.capacity
}

// Write appends raw RESP-encoded command bytes to the circular backlog.
// As bytes are written, currentOffset advances. If total bytes exceed capacity,
// older bytes are overwritten and firstOffset advances accordingly.
//
// Returns the new currentOffset after writing the data.
func (b *Backlog) Write(data []byte) int64 {
	if len(data) == 0 {
		b.mu.RLock()
		defer b.mu.RUnlock()
		return b.currentOffset
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	dataLen := int64(len(data))

	// If single write is larger than the entire buffer, discard the prefix that would
	// be immediately overwritten. Only retain the trailing `capacity` bytes.
	if dataLen >= b.capacity {
		discard := dataLen - b.capacity
		b.currentOffset += discard
		b.firstOffset = b.currentOffset
		data = data[discard:]
		dataLen = b.capacity
	}

	// Copy data into the circular buffer
	writePos := b.currentOffset % b.capacity
	spaceAtEnd := b.capacity - writePos

	if dataLen <= spaceAtEnd {
		// Fits in a single contiguous slice without wrapping
		copy(b.buffer[writePos:writePos+dataLen], data)
	} else {
		// Wraps around the end of the circular buffer
		copy(b.buffer[writePos:], data[:spaceAtEnd])
		copy(b.buffer[:dataLen-spaceAtEnd], data[spaceAtEnd:])
	}

	b.currentOffset += dataLen

	// Update firstOffset if the buffer has completely wrapped
	if b.currentOffset > b.capacity {
		b.firstOffset = b.currentOffset - b.capacity
	}

	return b.currentOffset
}

// CanPartialResync checks whether a replica's requested replID and offset can be satisfied
// from the in-memory backlog without needing a full snapshot resynchronization.
func (b *Backlog) CanPartialResync(reqReplID string, reqOffset int64) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// 1. Replication ID must match exactly
	if reqReplID != b.replID {
		return false
	}

	// 2. Requested offset cannot be in the future
	if reqOffset > b.currentOffset {
		return false
	}

	// 3. Requested offset must not have been overwritten
	if reqOffset < b.firstOffset {
		return false
	}

	return true
}

// GetSlice retrieves a contiguous slice of bytes starting from reqOffset up to currentOffset.
// Returns ErrReplIDMismatch if replID does not match, or ErrOffsetOutOfRange if reqOffset
// is not within [firstOffset, currentOffset].
func (b *Backlog) GetSlice(reqReplID string, reqOffset int64) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if reqReplID != b.replID {
		return nil, ErrReplIDMismatch
	}

	if reqOffset < b.firstOffset || reqOffset > b.currentOffset {
		return nil, fmt.Errorf("%w: requested %d, available range [%d, %d]",
			ErrOffsetOutOfRange, reqOffset, b.firstOffset, b.currentOffset)
	}

	length := b.currentOffset - reqOffset
	if length == 0 {
		return []byte{}, nil
	}

	result := make([]byte, length)
	readPos := reqOffset % b.capacity
	spaceAtEnd := b.capacity - readPos

	if length <= spaceAtEnd {
		copy(result, b.buffer[readPos:readPos+length])
	} else {
		copy(result[:spaceAtEnd], b.buffer[readPos:])
		copy(result[spaceAtEnd:], b.buffer[:length-spaceAtEnd])
	}

	return result, nil
}
