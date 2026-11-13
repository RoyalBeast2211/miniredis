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
func TestAOFRewriteEmptyDatabase(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "empty_rewrite.aof")

	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	aofWriter, err := Open(aofPath, SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}
	defer aofWriter.Close()

	// Write and delete a key so the AOF is non-empty while RAM is empty
	store.Set("temp", "val")
	_ = aofWriter.Write([]string{"SET", "temp", "val"})
	store.Delete("temp")
	_ = aofWriter.Write([]string{"DEL", "temp"})

	res, err := aofWriter.Rewrite(store, "")
	if err != nil {
		t.Fatalf("Rewrite on empty store failed: %v", err)
	}

	if res.KeysWritten != 0 || res.NewFileSize != 0 {
		t.Fatalf("Expected 0 keys and 0 bytes after rewrite, got keys=%d size=%d", res.KeysWritten, res.NewFileSize)
	}

	// Verify no temp file remains
	if _, err := os.Stat(aofPath + ".rewrite.tmp"); !os.IsNotExist(err) {
		t.Fatalf("Expected .rewrite.tmp to be cleaned up")
	}

	// Verify recovery from the empty rewritten AOF succeeds
	freshStore := storage.NewStoreWithInterval(0)
	defer freshStore.Close()
	recRes, err := Recover(aofPath, freshStore)
	if err != nil || recRes.CommandsReplayed != 0 {
		t.Fatalf("Expected clean 0-command recovery, got %+v (err=%v)", recRes, err)
	}
}

// TestAOFRewriteCompactionAndTTL verifies that:
// - Redundant SET updates collapse to a single SET.
// - Deleted keys are completely omitted.
// - Expired keys are excluded.
// - Active TTL keys preserve their exact absolute EXPIREAT deadline.
// - Persisted keys remain persistent without extra PERSIST commands.
func TestAOFRewriteCompactionAndTTL(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "compact.aof")

	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	aofWriter, err := Open(aofPath, SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}

	// 1. Update counter 50 times -> should compact to 1 SET
	for i := 1; i <= 50; i++ {
		val := fmt.Sprintf("%d", i)
		store.Set("counter", val)
		_ = aofWriter.Write([]string{"SET", "counter", val})
	}

	// 2. Key set and then deleted -> should compact to 0 commands
	store.Set("deletedKey", "gone")
	_ = aofWriter.Write([]string{"SET", "deletedKey", "gone"})
	store.Delete("deletedKey")
	_ = aofWriter.Write([]string{"DEL", "deletedKey"})

	// 3. Key with future absolute expiration -> should compact to SET + EXPIREAT
	futureDeadline := time.Now().Add(30 * time.Minute).Unix()
	store.Set("session", "abc123")
	store.ExpireAt("session", futureDeadline)
	_ = aofWriter.Write([]string{"SET", "session", "abc123"})
	_ = aofWriter.Write([]string{"EXPIREAT", "session", fmt.Sprintf("%d", futureDeadline)})

	// 4. Key with short expiration that expires BEFORE rewrite -> should be excluded
	store.Set("shortLived", "bye")
	store.Expire("shortLived", 1)
	_ = aofWriter.Write([]string{"SET", "shortLived", "bye"})
	_ = aofWriter.Write([]string{"EXPIREAT", "shortLived", fmt.Sprintf("%d", time.Now().Add(1*time.Second).Unix())})

	// 5. Key expired then persisted -> should compact to a single SET (no TTL)
	store.Set("permanent", "stay")
	store.Expire("permanent", 100)
	store.Persist("permanent")
	_ = aofWriter.Write([]string{"SET", "permanent", "stay"})
	_ = aofWriter.Write([]string{"EXPIREAT", "permanent", fmt.Sprintf("%d", time.Now().Add(100*time.Second).Unix())})
	_ = aofWriter.Write([]string{"PERSIST", "permanent"})

	// Record bloated AOF size before rewrite
	oldSize, err := aofWriter.Offset()
	if err != nil {
		t.Fatalf("Offset failed: %v", err)
	}

	// Wait for shortLived key to expire
	time.Sleep(1100 * time.Millisecond)

	// Perform AOF Rewrite
	res, err := aofWriter.Rewrite(store, "")
	if err != nil {
		t.Fatalf("Rewrite failed: %v", err)
	}

	// Must have written exactly 3 live keys: counter, session, permanent
	if res.KeysWritten != 3 {
		t.Fatalf("Expected 3 live keys written, got %d", res.KeysWritten)
	}
	if res.NewFileSize >= oldSize {
		t.Fatalf("Expected compacted AOF (%d bytes) to be smaller than original (%d bytes)", res.NewFileSize, oldSize)
	}

	// Verify we can still append new commands after rewrite
	store.Set("afterRewrite", "works")
	if err := aofWriter.Write([]string{"SET", "afterRewrite", "works"}); err != nil {
		t.Fatalf("Write after rewrite failed: %v", err)
	}
	_ = aofWriter.Close()

	// Verify exact command count in compacted AOF:
	// 3 SETs + 1 EXPIREAT (for session) + 1 SET (afterRewrite) = 5 commands total!
	commands := readAllAOFCommands(t, aofPath)
	if len(commands) != 5 {
		t.Fatalf("Expected 5 commands in compacted AOF, got %d: %v", len(commands), commands)
	}

	// Recover into a brand new Store and verify 100% state fidelity
	recovered := storage.NewStoreWithInterval(0)
	defer recovered.Close()

	if _, err := Recover(aofPath, recovered); err != nil {
		t.Fatalf("Recovery from rewritten AOF failed: %v", err)
	}

	if val, ok := recovered.Get("counter"); !ok || val != "50" {
		t.Errorf("Expected counter='50', got %q (ok=%v)", val, ok)
	}
	if recovered.Exists("deletedKey") {
		t.Errorf("Expected deletedKey to not exist")
	}
	if recovered.Exists("shortLived") {
		t.Errorf("Expected expired shortLived key to not exist")
	}
	if val, ok := recovered.Get("session"); !ok || val != "abc123" {
		t.Errorf("Expected session='abc123', got %q (ok=%v)", val, ok)
	}
	ttl := recovered.TTL("session")
	if ttl <= 0 || ttl > 1800 {
		t.Errorf("Expected session TTL to be preserved (~1800s), got %d", ttl)
	}
	if val, ok := recovered.Get("permanent"); !ok || val != "stay" || recovered.TTL("permanent") != -1 {
		t.Errorf("Expected permanent='stay' with TTL=-1, got val=%q ok=%v ttl=%d", val, ok, recovered.TTL("permanent"))
	}
	if val, ok := recovered.Get("afterRewrite"); !ok || val != "works" {
		t.Errorf("Expected afterRewrite='works', got %q (ok=%v)", val, ok)
	}
}

// TestAOFRewriteIncrementalOrdering verifies deterministically that concurrent writes
// arriving while the rewrite temp file is being written are captured in exact order
// without being lost or reordered.
func TestAOFRewriteIncrementalOrdering(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "incremental.aof")

	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	aofWriter, err := Open(aofPath, SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}

	// Initial state before rewrite starts
	store.Set("k1", "initial")
	_ = aofWriter.Write([]string{"SET", "k1", "initial"})
	store.Set("toDelete", "bye")
	_ = aofWriter.Write([]string{"SET", "toDelete", "bye"})

	// Inject writes specifically during the unlocked Phase 2 window (after snapshot, before swap)
	aofWriter.onRewriteBeforeSwap = func() {
		// Sequential updates to the same key to prove ordering is preserved
		store.Set("seqKey", "step1")
		_ = aofWriter.Write([]string{"SET", "seqKey", "step1"})

		store.Set("seqKey", "step2")
		_ = aofWriter.Write([]string{"SET", "seqKey", "step2"})

		store.Set("seqKey", "step3")
		_ = aofWriter.Write([]string{"SET", "seqKey", "step3"})

		// Delete a key that was present in the baseline snapshot
		store.Delete("toDelete")
		_ = aofWriter.Write([]string{"DEL", "toDelete"})
	}

	res, err := aofWriter.Rewrite(store, "")
	if err != nil {
		t.Fatalf("Rewrite failed: %v", err)
	}
	if res.IncrementalCommands != 4 {
		t.Fatalf("Expected 4 incremental commands drained, got %d", res.IncrementalCommands)
	}
	_ = aofWriter.Close()

	// Replay compacted AOF into fresh store
	recovered := storage.NewStoreWithInterval(0)
	defer recovered.Close()

	if _, err := Recover(aofPath, recovered); err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	if val, ok := recovered.Get("k1"); !ok || val != "initial" {
		t.Errorf("Expected k1='initial', got %q", val)
	}
	if val, ok := recovered.Get("seqKey"); !ok || val != "step3" {
		t.Errorf("Expected seqKey='step3' (strict order preserved), got %q (ok=%v)", val, ok)
	}
	if recovered.Exists("toDelete") {
		t.Errorf("Expected toDelete to be deleted by incremental DEL command")
	}
}

// TestAOFRewriteConcurrentStress tests concurrent goroutines writing to the AOF
// while Rewrite runs simultaneously, verifying zero data races and zero lost commands.
func TestAOFRewriteConcurrentStress(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "stress_rewrite.aof")

	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	aofWriter, err := Open(aofPath, SyncEverySecond)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}

	// Pre-populate 100 keys
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("base_%d", i)
		store.Set(k, "val")
		_ = aofWriter.Write([]string{"SET", k, "val"})
	}

	const numWriters = 10
	const writesPerWriter = 30

	var wg sync.WaitGroup
	wg.Add(numWriters + 1)

	// Concurrent writers inserting unique keys
	for w := 0; w < numWriters; w++ {
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < writesPerWriter; i++ {
				k := fmt.Sprintf("w%d_k%d", wid, i)
				v := fmt.Sprintf("v%d", i)
				store.Set(k, v)
				if err := aofWriter.Write([]string{"SET", k, v}); err != nil {
					t.Errorf("Concurrent write failed: %v", err)
				}
			}
		}(w)
	}

	// Concurrent rewrite
	go func() {
		defer wg.Done()
		if _, err := aofWriter.Rewrite(store, ""); err != nil {
			t.Errorf("Concurrent rewrite failed: %v", err)
		}
	}()

	wg.Wait()
	_ = aofWriter.Close()

	// Recover into a fresh store and verify EVERY SINGLE key (base + concurrent) survived!
	recovered := storage.NewStoreWithInterval(0)
	defer recovered.Close()

	if _, err := Recover(aofPath, recovered); err != nil {
		t.Fatalf("Recovery after concurrent stress rewrite failed: %v", err)
	}

	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("base_%d", i)
		if !recovered.Exists(k) {
			t.Errorf("Missing base key %s after concurrent rewrite", k)
		}
	}
	for w := 0; w < numWriters; w++ {
		for i := 0; i < writesPerWriter; i++ {
			k := fmt.Sprintf("w%d_k%d", w, i)
			if !recovered.Exists(k) {
				t.Errorf("Missing concurrent key %s after rewrite", k)
			}
		}
	}
}

// TestAOFRewritePreservesLRUOrder verifies that replaying a rewritten AOF under
// the same LRU capacity preserves live keys and recency order without logging artificial DELs.
func TestAOFRewritePreservesLRUOrder(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "lru_rewrite.aof")

	// Capacity = 3
	store := storage.NewStoreWithConfig(3, 0)
	defer store.Close()

	aofWriter, err := Open(aofPath, SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}

	// Insert k1, k2, k3 -> MRU: k3, k2, k1 :LRU
	store.Set("k1", "1")
	_ = aofWriter.Write([]string{"SET", "k1", "1"})
	store.Set("k2", "2")
	_ = aofWriter.Write([]string{"SET", "k2", "2"})
	store.Set("k3", "3")
	_ = aofWriter.Write([]string{"SET", "k3", "3"})

	// Access k1 -> MRU: k1, k3, k2 :LRU
	_, _ = store.Get("k1")

	// Insert k4 -> evicts k2 (LRU)! Live keys: MRU: k4, k1, k3 :LRU
	store.Set("k4", "4")
	_ = aofWriter.Write([]string{"SET", "k4", "4"})

	// Rewrite AOF -> should emit ONLY k3, k1, k4 (from LRU to MRU) and ZERO DEL commands!
	res, err := aofWriter.Rewrite(store, "")
	if err != nil {
		t.Fatalf("Rewrite failed: %v", err)
	}
	if res.KeysWritten != 3 {
		t.Fatalf("Expected 3 live keys written, got %d", res.KeysWritten)
	}
	_ = aofWriter.Close()

	// Verify no DEL commands were written
	commands := readAllAOFCommands(t, aofPath)
	if len(commands) != 3 {
		t.Fatalf("Expected exactly 3 SET commands, got %d: %v", len(commands), commands)
	}
	for _, cmd := range commands {
		if cmd[0] == "DEL" {
			t.Fatalf("LRU eviction must NOT be logged as DEL: %v", cmd)
		}
	}

	// Recover into a fresh capacity-3 store
	recovered := storage.NewStoreWithConfig(3, 0)
	defer recovered.Close()
	if _, err := Recover(aofPath, recovered); err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// k2 was evicted before rewrite; k3, k1, k4 must exist
	if recovered.Exists("k2") {
		t.Errorf("Expected evicted key k2 to not exist")
	}

	// Inserting k5 into recovered store must evict k3 (which was LRU before rewrite!)
	recovered.Set("k5", "5")
	if recovered.Exists("k3") {
		t.Errorf("Expected k3 to be evicted first, proving LRU order was preserved")
	}
	if !recovered.Exists("k1") || !recovered.Exists("k4") || !recovered.Exists("k5") {
		t.Errorf("Expected k1, k4, k5 to remain in store")
	}
}

// TestAOFRewriteFailedAndInterrupted verifies:
// 1. Rewriting a closed AOF fails cleanly.
// 2. A leftover .rewrite.tmp from an interrupted crash does not corrupt original AOF recovery.
func TestAOFRewriteFailedAndInterrupted(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "interrupted.aof")

	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	aofWriter, err := Open(aofPath, SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}

	store.Set("safeKey", "safeVal")
	_ = aofWriter.Write([]string{"SET", "safeKey", "safeVal"})
	_ = aofWriter.Close()

	// 1. Rewrite on closed AOF must return an error
	if _, err := aofWriter.Rewrite(store, ""); err == nil {
		t.Fatalf("Expected error rewriting a closed AOF, got nil")
	}

	// 2. Simulate an interrupted crash leaving a half-written .rewrite.tmp file
	tempPath := aofPath + ".rewrite.tmp"
	if err := os.WriteFile(tempPath, []byte("*3\r\n$3\r\nSET\r\n$4\r\nHALF"), 0644); err != nil {
		t.Fatalf("Failed to create simulated crash temp file: %v", err)
	}

	// Original AOF must recover cleanly, ignoring the orphaned .rewrite.tmp
	recovered := storage.NewStoreWithInterval(0)
	defer recovered.Close()
	if _, err := Recover(aofPath, recovered); err != nil {
		t.Fatalf("Recovery failed with orphaned temp file present: %v", err)
	}
	if val, ok := recovered.Get("safeKey"); !ok || val != "safeVal" {
		t.Fatalf("Expected safeKey='safeVal', got %q (ok=%v)", val, ok)
	}
}
