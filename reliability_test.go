package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"miniredis/aof"
	"miniredis/snapshot"
	"miniredis/storage"
)

// simulateRecovery runs the exact 5-step startup recovery sequence from main.go
// against the given snapshot and AOF paths.
func simulateRecovery(t *testing.T, aofPath, snapPath string) (*storage.Store, aof.RecoveryResult, error) {
	t.Helper()

	store := storage.NewStore()

	// Step 1: Load snapshot if it exists
	var startAOFOffset int64 = 0
	if snapPath != "" {
		header, snapEntries, err := snapshot.Load(snapPath)
		if err == nil {
			store.LoadSnapshot(snapEntries)
			startAOFOffset = header.AOFOffset
		} else if !os.IsNotExist(err) {
			store.Close()
			return nil, aof.RecoveryResult{}, fmt.Errorf("snapshot load failed: %w", err)
		}
	}

	// Step 2: Replay AOF records starting from snapshot offset
	recRes, err := aof.RecoverWithOffset(aofPath, store, startAOFOffset)
	if err != nil {
		store.Close()
		return nil, aof.RecoveryResult{}, fmt.Errorf("aof recovery failed: %w", err)
	}

	return store, recRes, nil
}

// TestCrashScenario1_RestartAfterNormalSET verifies that after standard SET operations
// and a process shutdown, all keys and values are faithfully recovered.
func TestCrashScenario1_RestartAfterNormalSET(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "scenario1.aof")

	// Phase 1: Server run with normal SETs
	store1 := storage.NewStore()
	aof1, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("failed to open aof: %v", err)
	}

	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("user:%d", i)
		v := fmt.Sprintf("val:%d", i)
		store1.Set(k, v)
		if err := aof1.Write([]string{"SET", k, v}); err != nil {
			t.Fatalf("aof write failed: %v", err)
		}
	}

	_ = aof1.Close()
	store1.Close()

	// Phase 2: Simulate process restart
	recoveredStore, recRes, err := simulateRecovery(t, aofPath, "")
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer recoveredStore.Close()

	if recRes.CommandsReplayed != 50 {
		t.Errorf("expected 50 commands replayed, got %d", recRes.CommandsReplayed)
	}

	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("user:%d", i)
		expectedVal := fmt.Sprintf("val:%d", i)
		val, exists := recoveredStore.Get(k)
		if !exists || val != expectedVal {
			t.Errorf("key %s: expected %q, got %q (exists=%v)", k, expectedVal, val, exists)
		}
	}
}

// TestCrashScenario2_RestartAfterMixedMutations verifies recovery across SET, DEL,
// EXPIRE, and PERSIST operations, verifying final convergence.
func TestCrashScenario2_RestartAfterMixedMutations(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "scenario2.aof")

	store1 := storage.NewStore()
	aof1, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("failed to open aof: %v", err)
	}

	// 1. SET k1, k2, k3
	store1.Set("k1", "v1")
	_ = aof1.Write([]string{"SET", "k1", "v1"})
	store1.Set("k2", "v2")
	_ = aof1.Write([]string{"SET", "k2", "v2"})
	store1.Set("k3", "v3")
	_ = aof1.Write([]string{"SET", "k3", "v3"})

	// 2. DEL k2
	store1.Delete("k2")
	_ = aof1.Write([]string{"DEL", "k2"})

	// 3. EXPIRE k1 for 10 minutes (preserved as absolute timestamp)
	expDeadline := time.Now().Add(10 * time.Minute).Unix()
	store1.ExpireAt("k1", expDeadline)
	_ = aof1.Write([]string{"EXPIREAT", "k1", fmt.Sprintf("%d", expDeadline)})

	// 4. EXPIRE k3 then PERSIST k3
	store1.ExpireAt("k3", expDeadline)
	_ = aof1.Write([]string{"EXPIREAT", "k3", fmt.Sprintf("%d", expDeadline)})
	store1.Persist("k3")
	_ = aof1.Write([]string{"PERSIST", "k3"})

	_ = aof1.Close()
	store1.Close()

	// Phase 2: Restart and recover
	recoveredStore, _, err := simulateRecovery(t, aofPath, "")
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer recoveredStore.Close()

	// k1 must exist with positive TTL
	if !recoveredStore.Exists("k1") {
		t.Errorf("expected k1 to exist")
	}
	if ttl := recoveredStore.TTL("k1"); ttl <= 0 {
		t.Errorf("expected positive TTL for k1, got %d", ttl)
	}

	// k2 must NOT exist
	if recoveredStore.Exists("k2") {
		t.Errorf("expected deleted key k2 to not exist")
	}

	// k3 must exist and be persistent (TTL == -1)
	if val, ok := recoveredStore.Get("k3"); !ok || val != "v3" {
		t.Errorf("expected k3=v3, got %q (ok=%v)", val, ok)
	}
	if ttl := recoveredStore.TTL("k3"); ttl != -1 {
		t.Errorf("expected TTL=-1 for persisted key k3, got %d", ttl)
	}
}

// TestCrashScenario3_RestartWithExpiredKeys asserts that expired keys NEVER become
// valid or resurrected upon server restart.
func TestCrashScenario3_RestartWithExpiredKeys(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "scenario3.aof")

	store1 := storage.NewStore()
	aof1, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("failed to open aof: %v", err)
	}

	// Set validKey
	store1.Set("validKey", "alive")
	_ = aof1.Write([]string{"SET", "validKey", "alive"})

	// Set expiredKey with a deadline 10 seconds in the PAST
	pastDeadline := time.Now().Add(-10 * time.Second).Unix()
	store1.Set("expiredKey", "ghost")
	_ = aof1.Write([]string{"SET", "expiredKey", "ghost"})
	_ = aof1.Write([]string{"EXPIREAT", "expiredKey", fmt.Sprintf("%d", pastDeadline)})

	_ = aof1.Close()
	store1.Close()

	// Restart and recover
	recoveredStore, _, err := simulateRecovery(t, aofPath, "")
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer recoveredStore.Close()

	if !recoveredStore.Exists("validKey") {
		t.Errorf("expected validKey to exist")
	}
	if recoveredStore.Exists("expiredKey") {
		t.Errorf("CRITICAL INVARIANT VIOLATION: expiredKey resurrected after restart!")
	}
	if val, ok := recoveredStore.Get("expiredKey"); ok || val != "" {
		t.Errorf("expected Get(expiredKey) to return empty, got %q (ok=%v)", val, ok)
	}
}

// TestCrashScenario4_RestartAfterSnapshot verifies snapshot restoration paired with
// incremental AOF recovery.
func TestCrashScenario4_RestartAfterSnapshot(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "scenario4.aof")
	snapPath := filepath.Join(tempDir, "dump.rdb")

	store1 := storage.NewStore()
	aof1, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("failed to open aof: %v", err)
	}

	// Write 10 keys before snapshot
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("pre:%d", i)
		store1.Set(k, "val")
		_ = aof1.Write([]string{"SET", k, "val"})
	}

	// Capture snapshot
	if err := takeSnapshot(store1, aof1, snapPath); err != nil {
		t.Fatalf("takeSnapshot failed: %v", err)
	}

	// Write 5 keys AFTER snapshot
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("post:%d", i)
		store1.Set(k, "val")
		_ = aof1.Write([]string{"SET", k, "val"})
	}

	_ = aof1.Close()
	store1.Close()

	// Restart and recover
	recoveredStore, recRes, err := simulateRecovery(t, aofPath, snapPath)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer recoveredStore.Close()

	// Exactly 5 incremental commands should be replayed from AOF
	if recRes.CommandsReplayed != 5 {
		t.Errorf("expected 5 commands replayed from AOF offset, got %d", recRes.CommandsReplayed)
	}

	// Verify all 15 keys exist
	for i := 0; i < 10; i++ {
		if !recoveredStore.Exists(fmt.Sprintf("pre:%d", i)) {
			t.Errorf("missing snapshot key pre:%d", i)
		}
	}
	for i := 0; i < 5; i++ {
		if !recoveredStore.Exists(fmt.Sprintf("post:%d", i)) {
			t.Errorf("missing incremental key post:%d", i)
		}
	}
}

// TestCrashScenario5_RestartAfterAOFRewrite verifies that after AOF compaction,
// the newly compacted log replays accurately.
func TestCrashScenario5_RestartAfterAOFRewrite(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "scenario5.aof")

	store1 := storage.NewStore()
	aof1, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("failed to open aof: %v", err)
	}

	// Overwrite the same key 20 times
	for i := 0; i < 20; i++ {
		store1.Set("counter", fmt.Sprintf("%d", i))
		_ = aof1.Write([]string{"SET", "counter", fmt.Sprintf("%d", i)})
	}

	// Perform rewrite
	res, err := aof1.Rewrite(store1, "")
	if err != nil {
		t.Fatalf("rewrite failed: %v", err)
	}
	if res.KeysWritten != 1 {
		t.Errorf("expected 1 key written in rewrite, got %d", res.KeysWritten)
	}

	_ = aof1.Close()
	store1.Close()

	// Restart and recover
	recoveredStore, recRes, err := simulateRecovery(t, aofPath, "")
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer recoveredStore.Close()

	// The rewritten log should only have 1 command
	if recRes.CommandsReplayed != 1 {
		t.Errorf("expected 1 command replayed from compacted AOF, got %d", recRes.CommandsReplayed)
	}
	if val, ok := recoveredStore.Get("counter"); !ok || val != "19" {
		t.Errorf("expected counter=19, got %q (ok=%v)", val, ok)
	}
}

// TestCrashScenario6_TornWriteTruncation verifies that a partially written final AOF
// record (caused by power loss / crash) is detected and safely truncated back to the last valid record.
func TestCrashScenario6_TornWriteTruncation(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "scenario6.aof")

	store1 := storage.NewStore()
	aof1, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("failed to open aof: %v", err)
	}

	// 2 complete, valid commands
	store1.Set("valid1", "data1")
	_ = aof1.Write([]string{"SET", "valid1", "data1"})
	store1.Set("valid2", "data2")
	_ = aof1.Write([]string{"SET", "valid2", "data2"})
	_ = aof1.Close()

	// Corrupt file by appending a torn/half-written command at the very end
	tornBytes := []byte("*3\r\n$3\r\nSET\r\n$6\r\ninval") // Cut off halfway
	file, err := os.OpenFile(aofPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open file for append: %v", err)
	}
	_, _ = file.Write(tornBytes)
	_ = file.Close()

	// Restart and recover: recovery must succeed, recover the 2 valid keys, and truncate the torn bytes!
	recoveredStore, recRes, err := simulateRecovery(t, aofPath, "")
	if err != nil {
		t.Fatalf("recovery failed on torn record: %v", err)
	}
	defer recoveredStore.Close()

	if recRes.CommandsReplayed != 2 {
		t.Errorf("expected 2 valid commands replayed, got %d", recRes.CommandsReplayed)
	}
	if recRes.TruncatedBytes != int64(len(tornBytes)) {
		t.Errorf("expected %d truncated bytes, got %d", len(tornBytes), recRes.TruncatedBytes)
	}

	// Verify valid keys survived
	if !recoveredStore.Exists("valid1") || !recoveredStore.Exists("valid2") {
		t.Errorf("valid keys missing after truncation")
	}
	if recoveredStore.Exists("inval") {
		t.Errorf("torn key 'inval' should not exist")
	}
}

// TestCrashScenario7_CorruptedAOFRejected verifies that syntax corruption in the
// middle of an AOF file causes recovery to return an explicit error rather than silently loading bad data.
func TestCrashScenario7_CorruptedAOFRejected(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "scenario7.aof")

	store1 := storage.NewStore()
	aof1, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("failed to open aof: %v", err)
	}

	store1.Set("k1", "v1")
	_ = aof1.Write([]string{"SET", "k1", "v1"})
	store1.Set("k2", "v2")
	_ = aof1.Write([]string{"SET", "k2", "v2"})
	_ = aof1.Close()

	// Intentionally corrupt bytes in the MIDDLE of the file
	fileData, err := os.ReadFile(aofPath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	// Corrupt byte index 10 (change RESP formatting byte to garbage)
	fileData[10] = 'X'
	if err := os.WriteFile(aofPath, fileData, 0644); err != nil {
		t.Fatalf("failed to write corrupted file: %v", err)
	}

	// Recovery must fail with a descriptive corruption error
	_, _, err = simulateRecovery(t, aofPath, "")
	if err == nil {
		t.Fatalf("CRITICAL SAFETY FAILURE: recovery silently succeeded on corrupted AOF file!")
	}
}

// TestCrashScenario8_InconsistentSnapshotAndAOF verifies that if the snapshot references
// an AOF offset greater than the actual AOF file size, recovery halts with a clear error.
func TestCrashScenario8_InconsistentSnapshotAndAOF(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "scenario8.aof")
	snapPath := filepath.Join(tempDir, "dump.rdb")

	// Snapshot references AOF offset 999999
	entries := []storage.SnapshotEntry{
		{Key: "k1", Value: "v1"},
	}
	if err := snapshot.Save(snapPath, 999999, entries); err != nil {
		t.Fatalf("failed to save snapshot: %v", err)
	}

	// But AOF file only contains 1 tiny record (~30 bytes)
	aof1, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("failed to open aof: %v", err)
	}
	_ = aof1.Write([]string{"SET", "k1", "v1"})
	_ = aof1.Close()

	// Recovery must detect the offset inconsistency and fail
	_, _, err = simulateRecovery(t, aofPath, snapPath)
	if err == nil {
		t.Fatalf("expected error for inconsistent AOF offset, got nil")
	}
}

// TestCrashScenario9_FaultInjectionSimulatedFailures tests resilience against injected
// operating system failures during persistence.
func TestCrashScenario9_FaultInjectionSimulatedFailures(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "scenario9.aof")
	snapPath := filepath.Join(tempDir, "scenario9_dump.rdb")

	store := storage.NewStore()
	defer store.Close()
	store.Set("safe_key", "safe_val")

	aofWriter, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("failed to open aof: %v", err)
	}
	defer aofWriter.Close()

	// 1. Fault injection before AOF write (simulating disk full / I/O error)
	injectedErr := errors.New("simulated disk full: ENOSPC")
	aofWriter.SetFaultHooks(func() error {
		return injectedErr
	}, nil, nil)

	err = aofWriter.Write([]string{"SET", "failing_key", "val"})
	if !errors.Is(err, injectedErr) {
		t.Errorf("expected injected error %v, got %v", injectedErr, err)
	}

	// Clear hook
	aofWriter.SetFaultHooks(nil, nil, nil)

	// Normal write succeeds
	if err := aofWriter.Write([]string{"SET", "safe_key", "safe_val"}); err != nil {
		t.Fatalf("normal write failed after hook reset: %v", err)
	}

	// 2. Fault injection before snapshot atomic rename
	snapshot.TestHookBeforeRename = func(tempPath string) error {
		return errors.New("simulated crash before atomic rename")
	}
	defer func() { snapshot.TestHookBeforeRename = nil }()

	err = snapshot.Save(snapPath, 0, store.SnapshotData())
	if err == nil {
		t.Fatalf("expected error from snapshot fault hook, got nil")
	}

	// Live snapshot file must NOT have been created
	if _, err := os.Stat(snapPath); !os.IsNotExist(err) {
		t.Errorf("expected live snapshot to not exist when rename failed")
	}

	// 3. Fault injection during AOF rewrite right before atomic rename
	aofWriter.SetFaultHooks(nil, nil, func(tempPath string) error {
		return errors.New("simulated crash before rewrite rename")
	})

	_, err = aofWriter.Rewrite(store, "")
	if err == nil {
		t.Fatalf("expected error from rewrite fault hook, got nil")
	}

	// Live active AOF must still exist and be intact
	if _, err := os.Stat(aofPath); err != nil {
		t.Errorf("live AOF was damaged by failed rewrite: %v", err)
	}
}
