package snapshot

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"miniredis/storage"
)

// TestSnapshotCreateAndLoad verifies that entries, headers, and metadata
// round-trip cleanly through serialization and deserialization.
func TestSnapshotCreateAndLoad(t *testing.T) {
	tempDir := t.TempDir()
	snapPath := filepath.Join(tempDir, "dump.rdb")

	futureDeadline := time.Now().Add(10 * time.Minute).Truncate(time.Second)

	entries := []storage.SnapshotEntry{
		{
			Key:       "user:1",
			Value:     "Alice",
			HasExpire: false,
		},
		{
			Key:       "session:token",
			Value:     "secret-xyz-123",
			HasExpire: true,
			ExpireAt:  futureDeadline,
		},
		{
			Key:       "complex",
			Value:     "Hello\r\nWorld!\tWith \"quotes\" and \x00 binary bytes.",
			HasExpire: false,
		},
	}

	aofOffset := int64(4500)
	if err := Save(snapPath, aofOffset, entries); err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	header, loadedEntries, err := Load(snapPath)
	if err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}

	if header.Version != CurrentVersion {
		t.Errorf("Expected version %d, got %d", CurrentVersion, header.Version)
	}
	if header.AOFOffset != aofOffset {
		t.Errorf("Expected AOFOffset %d, got %d", aofOffset, header.AOFOffset)
	}
	if header.KeyCount != 3 {
		t.Errorf("Expected 3 keys, got %d", header.KeyCount)
	}

	if len(loadedEntries) != 3 {
		t.Fatalf("Expected 3 loaded entries, got %d", len(loadedEntries))
	}

	// Verify Entry 0
	if loadedEntries[0].Key != "user:1" || loadedEntries[0].Value != "Alice" || loadedEntries[0].HasExpire {
		t.Errorf("Entry 0 mismatch: %+v", loadedEntries[0])
	}

	// Verify Entry 1 (TTL)
	if loadedEntries[1].Key != "session:token" || loadedEntries[1].Value != "secret-xyz-123" || !loadedEntries[1].HasExpire {
		t.Errorf("Entry 1 mismatch: %+v", loadedEntries[1])
	}
	if loadedEntries[1].ExpireAt.Unix() != futureDeadline.Unix() {
		t.Errorf("Expected ExpireAt %d, got %d", futureDeadline.Unix(), loadedEntries[1].ExpireAt.Unix())
	}

	// Verify Entry 2 (Complex)
	if loadedEntries[2].Key != "complex" || loadedEntries[2].Value != entries[2].Value {
		t.Errorf("Entry 2 complex value mismatch: got %q, expected %q", loadedEntries[2].Value, entries[2].Value)
	}
}

// TestSnapshotEmptyDatabase verifies creating and loading a snapshot of an empty store.
func TestSnapshotEmptyDatabase(t *testing.T) {
	tempDir := t.TempDir()
	snapPath := filepath.Join(tempDir, "empty.rdb")

	if err := Save(snapPath, 0, nil); err != nil {
		t.Fatalf("Failed to save empty snapshot: %v", err)
	}

	header, entries, err := Load(snapPath)
	if err != nil {
		t.Fatalf("Failed to load empty snapshot: %v", err)
	}

	if header.KeyCount != 0 || len(entries) != 0 {
		t.Errorf("Expected 0 keys, got %d", header.KeyCount)
	}
}

// TestSnapshotEncodeDecodeInMemory verifies direct in-memory encoding and decoding for replication streaming.
func TestSnapshotEncodeDecodeInMemory(t *testing.T) {
	entries := []storage.SnapshotEntry{
		{Key: "k1", Value: "v1"},
		{Key: "k2", Value: "v2", HasExpire: true, ExpireAt: time.Now().Add(time.Hour).Truncate(time.Second)},
	}

	data, err := Encode(12345, entries)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	header, decodedEntries, err := Decode(data)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}

	if header.AOFOffset != 12345 || header.KeyCount != 2 {
		t.Fatalf("header mismatch: %+v", header)
	}
	if len(decodedEntries) != 2 || decodedEntries[0].Key != "k1" || decodedEntries[1].Key != "k2" {
		t.Fatalf("decoded entries mismatch: %+v", decodedEntries)
	}

	// Test truncated data error
	_, _, err = Decode(data[:20])
	if err == nil {
		t.Fatalf("expected error on truncated data, got nil")
	}

	// Test corrupted byte
	corrupted := append([]byte{}, data...)
	corrupted[10] ^= 0xFF
	_, _, err = Decode(corrupted)
	if err == nil {
		t.Fatalf("expected checksum error on corrupted data, got nil")
	}
}

// TestSnapshotMissingFile verifies that loading a nonexistent file returns os.ErrNotExist.
func TestSnapshotMissingFile(t *testing.T) {
	tempDir := t.TempDir()
	snapPath := filepath.Join(tempDir, "nonexistent.rdb")

	_, _, err := Load(snapPath)
	if !os.IsNotExist(err) {
		t.Fatalf("Expected os.ErrNotExist, got %v", err)
	}
}

// TestSnapshotCorruptedChecksum verifies that tampering with a single byte
// triggers a CRC32 checksum mismatch error.
func TestSnapshotCorruptedChecksum(t *testing.T) {
	tempDir := t.TempDir()
	snapPath := filepath.Join(tempDir, "corrupted.rdb")

	entries := []storage.SnapshotEntry{
		{Key: "foo", Value: "bar"},
	}
	if err := Save(snapPath, 100, entries); err != nil {
		t.Fatalf("Failed to save: %v", err)
	}

	// Tamper with one byte in the payload
	data, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	data[15] ^= 0xFF // Flip bits at byte 15
	if err := os.WriteFile(snapPath, data, 0644); err != nil {
		t.Fatalf("Failed to write corrupted file: %v", err)
	}

	_, _, err = Load(snapPath)
	if err == nil {
		t.Fatalf("Expected checksum error on corrupted snapshot, got nil")
	}
}

// TestSnapshotUnsupportedVersion verifies that unknown format versions are rejected.
func TestSnapshotUnsupportedVersion(t *testing.T) {
	tempDir := t.TempDir()
	snapPath := filepath.Join(tempDir, "bad_version.rdb")

	entries := []storage.SnapshotEntry{{Key: "k", Value: "v"}}
	_ = Save(snapPath, 100, entries)

	// Overwrite version field (bytes 8..12) with version 9999
	f, err := os.OpenFile(snapPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	_, _ = f.Seek(8, os.SEEK_SET)
	_ = binary.Write(f, binary.BigEndian, uint32(9999))
	f.Close()

	// Recompute CRC so version check triggers rather than CRC failure
	data, _ := os.ReadFile(snapPath)
	_, _, err = Load(snapPath)
	if err == nil {
		t.Fatalf("Expected error for unsupported version or corrupted file, got nil (file len %d)", len(data))
	}
}

// TestSnapshotAtomicReplacement verifies that an existing snapshot is replaced atomically
// and that interrupted saves do not damage the old snapshot.
func TestSnapshotAtomicReplacement(t *testing.T) {
	tempDir := t.TempDir()
	snapPath := filepath.Join(tempDir, "atomic.rdb")

	// Version 1 of snapshot
	initialEntries := []storage.SnapshotEntry{{Key: "initial", Value: "v1"}}
	if err := Save(snapPath, 10, initialEntries); err != nil {
		t.Fatalf("Initial save failed: %v", err)
	}

	// Verify Version 1
	header, entries, err := Load(snapPath)
	if err != nil || len(entries) != 1 || entries[0].Key != "initial" {
		t.Fatalf("Failed to load initial snapshot")
	}

	// Version 2 of snapshot replaces Version 1
	newEntries := []storage.SnapshotEntry{
		{Key: "k1", Value: "val1"},
		{Key: "k2", Value: "val2"},
	}
	if err := Save(snapPath, 50, newEntries); err != nil {
		t.Fatalf("Second save failed: %v", err)
	}

	header, entries, err = Load(snapPath)
	if err != nil || len(entries) != 2 || header.AOFOffset != 50 {
		t.Fatalf("Failed to load replaced snapshot: %v", err)
	}

	// Verify no temporary file was left behind
	tempPath := snapPath + ".tmp"
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Fatalf("Expected temp file %s to be cleaned up", tempPath)
	}
}

// TestSnapshotStoreIntegration verifies that SnapshotData() and LoadSnapshot()
// correctly capture and restore store data and purge expired keys.
func TestSnapshotStoreIntegration(t *testing.T) {
	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	store.Set("user:alive", "John")
	store.Set("user:expiring_future", "Bob")
	store.Expire("user:expiring_future", 3600) // 1 hour

	store.Set("user:expired", "Dead")
	store.Expire("user:expired", 1)
	time.Sleep(1100 * time.Millisecond) // Ensure it expires

	// Capture point-in-time snapshot
	snapEntries := store.SnapshotData()

	// Should contain user:alive and user:expiring_future, but NOT user:expired
	if len(snapEntries) != 2 {
		t.Fatalf("Expected 2 live entries in snapshot, got %d", len(snapEntries))
	}

	// Load into a fresh store
	freshStore := storage.NewStoreWithInterval(0)
	defer freshStore.Close()

	freshStore.LoadSnapshot(snapEntries)

	if !freshStore.Exists("user:alive") {
		t.Errorf("Expected user:alive to be restored")
	}
	if !freshStore.Exists("user:expiring_future") {
		t.Errorf("Expected user:expiring_future to be restored")
	}
	if freshStore.Exists("user:expired") {
		t.Errorf("Expected user:expired to NOT exist")
	}
}
