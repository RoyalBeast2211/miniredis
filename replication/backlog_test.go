package replication

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestBacklog_BasicWriteAndRead(t *testing.T) {
	b := NewBacklog(1000)

	if len(b.ReplID()) != 40 {
		t.Fatalf("expected 40-character replID, got length %d", len(b.ReplID()))
	}
	if b.CurrentOffset() != 0 || b.FirstOffset() != 0 {
		t.Fatalf("expected initial offsets to be 0, got first=%d, current=%d", b.FirstOffset(), b.CurrentOffset())
	}

	data1 := []byte("SET key1 val1\r\n")
	newOffset := b.Write(data1)
	if newOffset != int64(len(data1)) {
		t.Fatalf("expected offset %d, got %d", len(data1), newOffset)
	}

	data2 := []byte("SET key2 val2\r\n")
	b.Write(data2)

	totalLen := int64(len(data1) + len(data2))
	if b.CurrentOffset() != totalLen {
		t.Fatalf("expected currentOffset %d, got %d", totalLen, b.CurrentOffset())
	}

	// Slice from 0 should return both commands
	sliceAll, err := b.GetSlice(b.ReplID(), 0)
	if err != nil {
		t.Fatalf("unexpected error getting slice: %v", err)
	}
	expectedAll := append(append([]byte{}, data1...), data2...)
	if !bytes.Equal(sliceAll, expectedAll) {
		t.Fatalf("expected %q, got %q", expectedAll, sliceAll)
	}

	// Slice from data1 length should return only data2
	slice2, err := b.GetSlice(b.ReplID(), int64(len(data1)))
	if err != nil {
		t.Fatalf("unexpected error getting slice2: %v", err)
	}
	if !bytes.Equal(slice2, data2) {
		t.Fatalf("expected %q, got %q", data2, slice2)
	}

	// Slice at current offset should return empty byte slice
	emptySlice, err := b.GetSlice(b.ReplID(), totalLen)
	if err != nil {
		t.Fatalf("unexpected error on empty slice: %v", err)
	}
	if len(emptySlice) != 0 {
		t.Fatalf("expected 0 bytes, got %d", len(emptySlice))
	}
}

func TestBacklog_WrapAround(t *testing.T) {
	// Small capacity buffer of 100 bytes
	capacity := int64(100)
	b := NewBacklog(capacity)

	// Write 60 bytes of 'A'
	aData := bytes.Repeat([]byte("A"), 60)
	b.Write(aData)

	// Write 60 bytes of 'B' (total 120 bytes, overwriting first 20 bytes of 'A')
	bData := bytes.Repeat([]byte("B"), 60)
	b.Write(bData)

	if b.CurrentOffset() != 120 {
		t.Fatalf("expected current offset 120, got %d", b.CurrentOffset())
	}
	if b.FirstOffset() != 20 {
		t.Fatalf("expected first offset 20, got %d", b.FirstOffset())
	}

	// Offset 10 is expired (overwritten)
	if b.CanPartialResync(b.ReplID(), 10) {
		t.Fatalf("expected offset 10 to be ineligible for partial resync")
	}
	_, err := b.GetSlice(b.ReplID(), 10)
	if err == nil {
		t.Fatalf("expected error for expired offset 10, got nil")
	}

	// Offset 20 is the exact boundary
	if !b.CanPartialResync(b.ReplID(), 20) {
		t.Fatalf("expected boundary offset 20 to be eligible")
	}
	sliceFrom20, err := b.GetSlice(b.ReplID(), 20)
	if err != nil {
		t.Fatalf("unexpected error at boundary: %v", err)
	}
	if int64(len(sliceFrom20)) != 100 {
		t.Fatalf("expected 100 bytes in sliceFrom20, got %d", len(sliceFrom20))
	}

	// Verify contents: 40 'A's followed by 60 'B's
	expected := append(bytes.Repeat([]byte("A"), 40), bytes.Repeat([]byte("B"), 60)...)
	if !bytes.Equal(sliceFrom20, expected) {
		t.Fatalf("wrapped slice contents mismatch")
	}

	// Offset 80: in the middle of 'B's
	sliceFrom80, err := b.GetSlice(b.ReplID(), 80)
	if err != nil {
		t.Fatalf("unexpected error at offset 80: %v", err)
	}
	if !bytes.Equal(sliceFrom80, bytes.Repeat([]byte("B"), 40)) {
		t.Fatalf("expected 40 'B's, got %q", sliceFrom80)
	}
}

func TestBacklog_LargeSingleWrite(t *testing.T) {
	capacity := int64(50)
	b := NewBacklog(capacity)

	// Write 120 bytes in a single call (larger than capacity)
	bigData := make([]byte, 120)
	for i := 0; i < 120; i++ {
		bigData[i] = byte(i)
	}

	b.Write(bigData)

	if b.CurrentOffset() != 120 {
		t.Fatalf("expected current offset 120, got %d", b.CurrentOffset())
	}
	if b.FirstOffset() != 70 {
		t.Fatalf("expected first offset 70, got %d", b.FirstOffset())
	}

	slice, err := b.GetSlice(b.ReplID(), 70)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(slice, bigData[70:]) {
		t.Fatalf("expected trailing 50 bytes of bigData")
	}
}

func TestBacklog_CanPartialResync_Validation(t *testing.T) {
	b := NewBacklog(100)
	b.Write([]byte("HELLO"))

	// Wrong ReplID
	if b.CanPartialResync("wrong-id", 0) {
		t.Fatalf("expected false for wrong replID")
	}
	_, err := b.GetSlice("wrong-id", 0)
	if err != ErrReplIDMismatch {
		t.Fatalf("expected ErrReplIDMismatch, got %v", err)
	}

	// Future offset
	if b.CanPartialResync(b.ReplID(), 999) {
		t.Fatalf("expected false for future offset")
	}
	_, err = b.GetSlice(b.ReplID(), 999)
	if err == nil {
		t.Fatalf("expected error for future offset, got nil")
	}

	// Negative offset
	if b.CanPartialResync(b.ReplID(), -1) {
		t.Fatalf("expected false for negative offset")
	}
}

func TestBacklog_ConcurrentWritesAndReads(t *testing.T) {
	b := NewBacklog(1024)
	var wg sync.WaitGroup

	// 5 concurrent writers
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				msg := fmt.Sprintf("W%d:%d\n", workerID, i)
				b.Write([]byte(msg))
			}
		}(w)
	}

	// 5 concurrent readers
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				first := b.FirstOffset()
				cur := b.CurrentOffset()
				if cur > first {
					_, _ = b.GetSlice(b.ReplID(), first)
				}
			}
		}()
	}

	wg.Wait()
}
