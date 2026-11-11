package aof

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"miniredis/protocol"
	"miniredis/storage"
)

// readAllAOFCommands reads and parses all RESP commands recorded in an AOF file.
func readAllAOFCommands(t *testing.T, path string) [][]string {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open AOF file for reading: %v", err)
	}
	defer file.Close()

	reader := protocol.NewReader(file)
	var commands [][]string

	for {
		val, err := reader.ReadValue()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Error reading AOF command: %v", err)
		}

		cmd, err := val.ToCommand()
		if err != nil {
			t.Fatalf("Failed to convert RESP value to command: %v", err)
		}
		commands = append(commands, cmd)
	}

	return commands
}

// TestAOFBasicCommands tests creating an AOF and appending SET, DEL, PERSIST, and EXPIREAT.
func TestAOFBasicCommands(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "test.aof")

	aofWriter, err := Open(aofPath, SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}

	// 1. Append SET
	if err := aofWriter.Write([]string{"SET", "user:1", "Alice"}); err != nil {
		t.Fatalf("Failed to write SET: %v", err)
	}

	// 2. Append EXPIREAT (absolute deadline preservation)
	deadline := fmt.Sprintf("%d", time.Now().Add(60*time.Second).Unix())
	if err := aofWriter.Write([]string{"EXPIREAT", "user:1", deadline}); err != nil {
		t.Fatalf("Failed to write EXPIREAT: %v", err)
	}

	// 3. Append PERSIST
	if err := aofWriter.Write([]string{"PERSIST", "user:1"}); err != nil {
		t.Fatalf("Failed to write PERSIST: %v", err)
	}

	// 4. Append DEL
	if err := aofWriter.Write([]string{"DEL", "user:1"}); err != nil {
		t.Fatalf("Failed to write DEL: %v", err)
	}

	if err := aofWriter.Close(); err != nil {
		t.Fatalf("Failed to close AOF: %v", err)
	}

	commands := readAllAOFCommands(t, aofPath)
	if len(commands) != 4 {
		t.Fatalf("Expected 4 commands in AOF, got %d", len(commands))
	}

	if commands[0][0] != "SET" || commands[0][1] != "user:1" || commands[0][2] != "Alice" {
		t.Errorf("Unexpected SET command: %v", commands[0])
	}
	if commands[1][0] != "EXPIREAT" || commands[1][1] != "user:1" || commands[1][2] != deadline {
		t.Errorf("Unexpected EXPIREAT command: %v", commands[1])
	}
	if commands[2][0] != "PERSIST" || commands[2][1] != "user:1" {
		t.Errorf("Unexpected PERSIST command: %v", commands[2])
	}
	if commands[3][0] != "DEL" || commands[3][1] != "user:1" {
		t.Errorf("Unexpected DEL command: %v", commands[3])
	}
}

// TestAOFSpecialCharacters verifies that bulk string encoding safely handles
// strings containing spaces, tabs, newlines, binary sequences, and quotes.
func TestAOFSpecialCharacters(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "special.aof")

	aofWriter, err := Open(aofPath, SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}

	complexVal := "Hello\r\nWorld!\nWith spaces, tabs\t and \"quotes\"."
	if err := aofWriter.Write([]string{"SET", "complexKey", complexVal}); err != nil {
		t.Fatalf("Failed to write complex command: %v", err)
	}
	aofWriter.Close()

	commands := readAllAOFCommands(t, aofPath)
	if len(commands) != 1 {
		t.Fatalf("Expected 1 command, got %d", len(commands))
	}
	if commands[0][2] != complexVal {
		t.Fatalf("Expected complex value to match exactly, got %q", commands[0][2])
	}
}

// TestAOFWriteFailureAfterClose tests that attempting to write after Close returns an error.
func TestAOFWriteFailureAfterClose(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "closed.aof")

	aofWriter, err := Open(aofPath, SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}
	_ = aofWriter.Close()

	err = aofWriter.Write([]string{"SET", "k", "v"})
	if err == nil {
		t.Fatalf("Expected error writing to closed AOF, got nil")
	}

	err = aofWriter.Sync()
	if err == nil {
		t.Fatalf("Expected error syncing closed AOF, got nil")
	}
}

// TestAOFSyncPolicies verifies that Open and Close work correctly across all sync policies.
func TestAOFSyncPolicies(t *testing.T) {
	policies := []SyncPolicy{SyncAlways, SyncEverySecond, SyncNo}

	for _, policy := range policies {
		t.Run(string(policy), func(t *testing.T) {
			tempDir := t.TempDir()
			aofPath := filepath.Join(tempDir, "policy.aof")

			aofWriter, err := Open(aofPath, policy)
			if err != nil {
				t.Fatalf("Failed to open with policy %s: %v", policy, err)
			}

			for i := 0; i < 5; i++ {
				key := fmt.Sprintf("k%d", i)
				val := fmt.Sprintf("v%d", i)
				if err := aofWriter.Write([]string{"SET", key, val}); err != nil {
					t.Fatalf("Write failed: %v", err)
				}
			}

			if err := aofWriter.Sync(); err != nil {
				t.Fatalf("Sync failed: %v", err)
			}

			if err := aofWriter.Close(); err != nil {
				t.Fatalf("Close failed: %v", err)
			}

			commands := readAllAOFCommands(t, aofPath)
			if len(commands) != 5 {
				t.Fatalf("Expected 5 commands, got %d", len(commands))
			}
		})
	}
}

// TestAOFConcurrentWrites verifies that concurrent client writes do not corrupt the stream.
func TestAOFConcurrentWrites(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "concurrent.aof")

	aofWriter, err := Open(aofPath, SyncEverySecond)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}

	numGoroutines := 30
	writesPerGoroutine := 50
	totalWrites := numGoroutines * writesPerGoroutine

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for w := 0; w < writesPerGoroutine; w++ {
				key := fmt.Sprintf("g%d_k%d", gid, w)
				val := fmt.Sprintf("val_%d", w)
				if err := aofWriter.Write([]string{"SET", key, val}); err != nil {
					t.Errorf("Concurrent write failed: %v", err)
				}
			}
		}(g)
	}

	wg.Wait()
	_ = aofWriter.Close()

	commands := readAllAOFCommands(t, aofPath)
	if len(commands) != totalWrites {
		t.Fatalf("Expected %d total commands, got %d", totalWrites, len(commands))
	}
}

// TestAOFRecoveryEmptyAndMissing verifies behavior on missing or empty AOF files.
func TestAOFRecoveryEmptyAndMissing(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Missing file: should succeed cleanly with 0 replayed commands
	missingPath := filepath.Join(tempDir, "missing.aof")
	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	res, err := Recover(missingPath, store)
	if err != nil {
		t.Fatalf("Recover on missing file failed: %v", err)
	}
	if res.CommandsReplayed != 0 {
		t.Fatalf("Expected 0 commands replayed from missing file, got %d", res.CommandsReplayed)
	}

	// 2. Empty file: should succeed with 0 replayed commands
	emptyPath := filepath.Join(tempDir, "empty.aof")
	f, _ := os.Create(emptyPath)
	f.Close()

	res, err = Recover(emptyPath, store)
	if err != nil {
		t.Fatalf("Recover on empty file failed: %v", err)
	}
	if res.CommandsReplayed != 0 {
		t.Fatalf("Expected 0 commands replayed from empty file, got %d", res.CommandsReplayed)
	}
}

// TestAOFRecoveryBasicSequence tests replay of SET, updates, DEL, PERSIST, and EXPIREAT.
func TestAOFRecoveryBasicSequence(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "replay_seq.aof")

	// 1. Write commands to AOF file
	aofWriter, err := Open(aofPath, SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}

	// SET k1 v1
	_ = aofWriter.Write([]string{"SET", "k1", "v1"})
	// SET k2 v2
	_ = aofWriter.Write([]string{"SET", "k2", "v2"})
	// Update k1 -> v1_updated
	_ = aofWriter.Write([]string{"SET", "k1", "v1_updated"})
	// DEL k2
	_ = aofWriter.Write([]string{"DEL", "k2"})
	// SET k3 v3 with future expiration (1 hour from now)
	futureDeadline := fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix())
	_ = aofWriter.Write([]string{"SET", "k3", "v3"})
	_ = aofWriter.Write([]string{"EXPIREAT", "k3", futureDeadline})
	// SET k4 v4 with past expiration (10 minutes ago)
	pastDeadline := fmt.Sprintf("%d", time.Now().Add(-10*time.Minute).Unix())
	_ = aofWriter.Write([]string{"SET", "k4", "v4"})
	_ = aofWriter.Write([]string{"EXPIREAT", "k4", pastDeadline})
	// SET k5 v5, expire it, then PERSIST it
	_ = aofWriter.Write([]string{"SET", "k5", "v5"})
	_ = aofWriter.Write([]string{"EXPIREAT", "k5", futureDeadline})
	_ = aofWriter.Write([]string{"PERSIST", "k5"})

	_ = aofWriter.Close()

	// 2. Replay into a brand new Store
	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	res, err := Recover(aofPath, store)
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	if res.CommandsReplayed != 11 {
		t.Fatalf("Expected 11 commands replayed, got %d", res.CommandsReplayed)
	}

	// Verify k1 is updated
	val, ok := store.Get("k1")
	if !ok || val != "v1_updated" {
		t.Errorf("Expected k1='v1_updated', got %q (ok=%v)", val, ok)
	}

	// Verify k2 is deleted
	if store.Exists("k2") {
		t.Errorf("Expected k2 to be deleted")
	}

	// Verify k3 has active future TTL
	ttl := store.TTL("k3")
	if ttl <= 0 || ttl > 3600 {
		t.Errorf("Expected k3 TTL to be ~3600 seconds, got %d", ttl)
	}

	// Verify k4 (expired in past) was purged during replay
	if store.Exists("k4") {
		t.Errorf("Expected k4 with past deadline to be deleted during replay")
	}

	// Verify k5 is persistent (TTL = -1)
	if ttl := store.TTL("k5"); ttl != -1 {
		t.Errorf("Expected k5 TTL=-1 (persistent), got %d", ttl)
	}
}

// TestAOFRecoveryWithCapacityLimit tests replay under a fixed LRU capacity.
func TestAOFRecoveryWithCapacityLimit(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "lru_recovery.aof")

	aofWriter, err := Open(aofPath, SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}

	// Write 5 keys
	for i := 1; i <= 5; i++ {
		_ = aofWriter.Write([]string{"SET", fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)})
	}
	_ = aofWriter.Close()

	// Replay into a store with capacity = 3
	store := storage.NewStoreWithConfig(3, 0)
	defer store.Close()

	res, err := Recover(aofPath, store)
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	if res.CommandsReplayed != 5 {
		t.Fatalf("Expected 5 commands replayed, got %d", res.CommandsReplayed)
	}

	// Oldest keys k1 and k2 must have been evicted; k3, k4, k5 remain
	if store.Exists("k1") || store.Exists("k2") {
		t.Errorf("Expected k1 and k2 to be evicted under capacity 3")
	}
	if !store.Exists("k3") || !store.Exists("k4") || !store.Exists("k5") {
		t.Errorf("Expected k3, k4, k5 to be present")
	}
}

// TestAOFRecoveryIncompleteFinalRecord tests safe truncation of a partial command
// caused by an abrupt crash during write at the end of the file.
func TestAOFRecoveryIncompleteFinalRecord(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "truncated.aof")

	aofWriter, err := Open(aofPath, SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}

	// Write 2 valid commands
	_ = aofWriter.Write([]string{"SET", "valid1", "data1"})
	_ = aofWriter.Write([]string{"SET", "valid2", "data2"})
	_ = aofWriter.Close()

	// Append a truncated / incomplete command (e.g. power lost midway)
	f, err := os.OpenFile(aofPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open for append: %v", err)
	}
	// Incomplete RESP array: claims 3 elements, only provides 2 and no trailing CRLF
	_, _ = f.WriteString("*3\r\n$3\r\nSET\r\n$6\r\npartia")
	f.Close()

	// Run recovery
	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	res, err := Recover(aofPath, store)
	if err != nil {
		t.Fatalf("Expected recovery to truncate partial trailing record, got error: %v", err)
	}

	if res.CommandsReplayed != 2 {
		t.Fatalf("Expected 2 valid commands replayed, got %d", res.CommandsReplayed)
	}
	if res.TruncatedBytes == 0 {
		t.Fatalf("Expected TruncatedBytes > 0, got %d", res.TruncatedBytes)
	}

	// Verify valid keys were loaded
	if !store.Exists("valid1") || !store.Exists("valid2") {
		t.Errorf("Expected valid1 and valid2 to exist")
	}

	// Verify that reopening AOF in append mode and writing new commands works seamlessly
	aofWriter2, err := Open(aofPath, SyncAlways)
	if err != nil {
		t.Fatalf("Failed to reopen truncated AOF: %v", err)
	}
	if err := aofWriter2.Write([]string{"SET", "valid3", "data3"}); err != nil {
		t.Fatalf("Failed to write to truncated AOF: %v", err)
	}
	_ = aofWriter2.Close()

	commands := readAllAOFCommands(t, aofPath)
	if len(commands) != 3 {
		t.Fatalf("Expected 3 commands in cleanly truncated AOF, got %d", len(commands))
	}
}

// TestAOFRecoveryCorruptedMiddleFile tests that corruption in the middle of the file
// is not silently ignored and results in a fatal error.
func TestAOFRecoveryCorruptedMiddleFile(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "corrupted_middle.aof")

	f, err := os.Create(aofPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	// First command: valid
	f.WriteString("*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n")
	// Middle: garbage/corrupted bytes
	f.WriteString("?CORRUPTED_GARBAGE_BYTES\r\n")
	// Second command: valid
	f.WriteString("*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n")
	f.Close()

	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	_, err = Recover(aofPath, store)
	if err == nil {
		t.Fatalf("Expected error on corrupted middle file, got nil")
	}
}

// TestAOFRewriteEmptyDatabase verifies rewriting when the store contains 0 keys.
