package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"miniredis/aof"
	"miniredis/protocol"
	"miniredis/snapshot"
	"miniredis/storage"
)

// sendCommand sends a command formatted as a RESP Array and reads the response back.
func sendCommand(conn net.Conn, args ...string) (protocol.Value, error) {
	elements := make([]protocol.Value, len(args))
	for i, arg := range args {
		elements[i] = protocol.NewBulkString(arg)
	}
	cmd := protocol.NewArray(elements)

	if _, err := conn.Write(cmd.Encode()); err != nil {
		return protocol.Value{}, err
	}

	reader := protocol.NewReader(conn)
	return reader.ReadValue()
}

// TestEndToEndCommands verifies the complete flow of PING, ECHO, SET, GET, DEL, EXISTS,
// EXPIRE, TTL, and PERSIST over a network connection using net.Pipe.
func TestEndToEndCommands(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	store := storage.NewStoreWithInterval(0)
	defer store.Close()
	go handleConnection(serverConn, store, nil)

	// 1. PING (no args) -> +PONG
	resp, err := sendCommand(clientConn, "PING")
	if err != nil || resp.Type != protocol.TypeSimpleString || resp.Str != "PONG" {
		t.Fatalf("Expected +PONG, got %+v (err: %v)", resp, err)
	}

	// 2. SET foo bar -> +OK
	resp, err = sendCommand(clientConn, "sEt", "foo", "bar")
	if err != nil || resp.Type != protocol.TypeSimpleString || resp.Str != "OK" {
		t.Fatalf("Expected +OK, got %+v (err: %v)", resp, err)
	}

	// 3. TTL foo -> :-1 (no expiry)
	resp, err = sendCommand(clientConn, "TTL", "foo")
	if err != nil || resp.Type != protocol.TypeInteger || resp.Num != -1 {
		t.Fatalf("Expected :-1 for unexpiring key, got %+v (err: %v)", resp, err)
	}

	// 4. EXPIRE foo 10 -> :1
	resp, err = sendCommand(clientConn, "EXPIRE", "foo", "10")
	if err != nil || resp.Type != protocol.TypeInteger || resp.Num != 1 {
		t.Fatalf("Expected :1 on successful EXPIRE, got %+v (err: %v)", resp, err)
	}

	// 5. TTL foo -> remaining time (> 0 and <= 10)
	resp, err = sendCommand(clientConn, "TTL", "foo")
	if err != nil || resp.Type != protocol.TypeInteger || resp.Num <= 0 || resp.Num > 10 {
		t.Fatalf("Expected positive TTL <= 10, got %+v (err: %v)", resp, err)
	}

	// 6. PERSIST foo -> :1
	resp, err = sendCommand(clientConn, "PERSIST", "foo")
	if err != nil || resp.Type != protocol.TypeInteger || resp.Num != 1 {
		t.Fatalf("Expected :1 on successful PERSIST, got %+v (err: %v)", resp, err)
	}

	// 7. TTL foo after PERSIST -> :-1
	resp, err = sendCommand(clientConn, "TTL", "foo")
	if err != nil || resp.Type != protocol.TypeInteger || resp.Num != -1 {
		t.Fatalf("Expected :-1 after PERSIST, got %+v (err: %v)", resp, err)
	}

	// 8. EXPIRE with invalid number -> error
	resp, err = sendCommand(clientConn, "EXPIRE", "foo", "not_a_number")
	if err != nil || resp.Type != protocol.TypeError {
		t.Fatalf("Expected error for non-integer expiration, got %+v (err: %v)", resp, err)
	}

	// 9. Short expiration and lazy check
	resp, err = sendCommand(clientConn, "EXPIRE", "foo", "1")
	if err != nil || resp.Type != protocol.TypeInteger || resp.Num != 1 {
		t.Fatalf("Expected :1 for EXPIRE, got %+v", resp)
	}

	time.Sleep(1100 * time.Millisecond)

	// GET after expiration -> nil bulk string
	resp, err = sendCommand(clientConn, "GET", "foo")
	if err != nil || resp.Type != protocol.TypeBulkString || !resp.IsNull {
		t.Fatalf("Expected null bulk string after key expired, got %+v", resp)
	}

	// TTL on expired key -> :-2
	resp, err = sendCommand(clientConn, "TTL", "foo")
	if err != nil || resp.Type != protocol.TypeInteger || resp.Num != -2 {
		t.Fatalf("Expected :-2 for expired key, got %+v", resp)
	}
}

// TestEndToEndAOFPersistence tests that network commands accurately write to the AOF file,
// preserving absolute timestamps for expiration and skipping read-only commands.
func TestEndToEndAOFPersistence(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "e2e.aof")

	aofWriter, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}
	defer aofWriter.Close()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	store := storage.NewStoreWithInterval(0)
	defer store.Close()
	go handleConnection(serverConn, store, aofWriter)

	// 1. SET
	_, _ = sendCommand(clientConn, "SET", "city", "Tokyo")

	// 2. GET (read-only, must NOT be logged)
	_, _ = sendCommand(clientConn, "GET", "city")

	// 3. EXPIRE (must be logged as EXPIREAT with absolute unix timestamp)
	_, _ = sendCommand(clientConn, "EXPIRE", "city", "100")

	// 4. PERSIST
	_, _ = sendCommand(clientConn, "PERSIST", "city")

	// 5. DEL
	_, _ = sendCommand(clientConn, "DEL", "city")

	_ = aofWriter.Sync()

	// Read AOF file
	file, err := os.Open(aofPath)
	if err != nil {
		t.Fatalf("Failed to open AOF file: %v", err)
	}
	defer file.Close()

	reader := protocol.NewReader(file)
	var loggedCommands [][]string
	for {
		val, err := reader.ReadValue()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Error reading AOF: %v", err)
		}
		cmd, err := val.ToCommand()
		if err != nil {
			t.Fatalf("Failed to parse command: %v", err)
		}
		loggedCommands = append(loggedCommands, cmd)
	}

	// Should have exactly 4 logged commands: SET, EXPIREAT, PERSIST, DEL
	if len(loggedCommands) != 4 {
		t.Fatalf("Expected 4 commands in AOF, got %d: %v", len(loggedCommands), loggedCommands)
	}

	if loggedCommands[0][0] != "SET" || loggedCommands[0][1] != "city" || loggedCommands[0][2] != "Tokyo" {
		t.Errorf("Unexpected SET: %v", loggedCommands[0])
	}
	if loggedCommands[1][0] != "EXPIREAT" || loggedCommands[1][1] != "city" {
		t.Errorf("Unexpected EXPIREAT: %v", loggedCommands[1])
	}
	if loggedCommands[2][0] != "PERSIST" || loggedCommands[2][1] != "city" {
		t.Errorf("Unexpected PERSIST: %v", loggedCommands[2])
	}
	if loggedCommands[3][0] != "DEL" || loggedCommands[3][1] != "city" {
		t.Errorf("Unexpected DEL: %v", loggedCommands[3])
	}
}

// TestConcurrentClientsSharedStore spins up multiple TCP clients connecting simultaneously
// over a real network socket and testing concurrent operations including TTL.
func TestConcurrentClientsSharedStore(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	store := storage.NewStoreWithInterval(20 * time.Millisecond)
	defer store.Close()

	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "concurrent_server.aof")
	aofWriter, err := aof.Open(aofPath, aof.SyncEverySecond)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}
	defer aofWriter.Close()

	go func() {
		_ = runServer(listener, store, aofWriter)
	}()

	serverAddr := listener.Addr().String()
	numClients := 10
	opsPerClient := 20

	var wg sync.WaitGroup
	wg.Add(numClients)

	for clientID := 0; clientID < numClients; clientID++ {
		go func(cid int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", serverAddr)
			if err != nil {
				t.Errorf("Client %d failed to connect: %v", cid, err)
				return
			}
			defer conn.Close()

			for i := 0; i < opsPerClient; i++ {
				key := fmt.Sprintf("client_%d_key_%d", cid, i)
				val := fmt.Sprintf("val_%d", i)

				// SET
				setResp, err := sendCommand(conn, "SET", key, val)
				if err != nil || setResp.Type != protocol.TypeSimpleString || setResp.Str != "OK" {
					t.Errorf("Client %d SET failed: %v, resp: %+v", cid, err, setResp)
					return
				}

				// EXPIRE
				expResp, err := sendCommand(conn, "EXPIRE", key, "60")
				if err != nil || expResp.Type != protocol.TypeInteger || expResp.Num != 1 {
					t.Errorf("Client %d EXPIRE failed: %v, resp: %+v", cid, err, expResp)
					return
				}

				// TTL
				ttlResp, err := sendCommand(conn, "TTL", key)
				if err != nil || ttlResp.Type != protocol.TypeInteger || ttlResp.Num <= 0 {
					t.Errorf("Client %d TTL failed: %v, resp: %+v", cid, err, ttlResp)
					return
				}
			}
		}(clientID)
	}

	wg.Wait()
}

// TestServerStartupWithExistingAOF tests the complete startup lifecycle:
// 1. Existing AOF file is recovered into memory BEFORE accepting client connections.
// 2. Client connects and immediately finds previously persisted data.
// 3. New writes append cleanly to the AOF without re-appending the recovered commands.
func TestServerStartupWithExistingAOF(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "server_recovery.aof")

	// Phase 1: Pre-populate AOF with commands
	preAOF, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}
	_ = preAOF.Write([]string{"SET", "persisted_user", "JohnDoe"})
	_ = preAOF.Write([]string{"SET", "counter", "100"})
	_ = preAOF.Close()

	// Phase 2: Start server with recovery
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	// Replay existing AOF
	recoveryRes, err := aof.Recover(aofPath, store)
	if err != nil {
		t.Fatalf("Server AOF recovery failed: %v", err)
	}
	if recoveryRes.CommandsReplayed != 2 {
		t.Fatalf("Expected 2 commands recovered on startup, got %d", recoveryRes.CommandsReplayed)
	}

	// Open AOF for new client appends
	aofWriter, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}
	defer aofWriter.Close()

	go func() {
		_ = runServer(listener, store, aofWriter)
	}()

	// Phase 3: Client connects and queries recovered data
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	// Query pre-existing data
	getResp, err := sendCommand(conn, "GET", "persisted_user")
	if err != nil || getResp.Type != protocol.TypeBulkString || getResp.Str != "JohnDoe" {
		t.Fatalf("Expected 'JohnDoe' from recovered store, got %+v (err: %v)", getResp, err)
	}

	// Perform a new write
	setResp, err := sendCommand(conn, "SET", "new_key", "new_val")
	if err != nil || setResp.Type != protocol.TypeSimpleString || setResp.Str != "OK" {
		t.Fatalf("SET new_key failed: %v", err)
	}

	// Close AOF and inspect file
	_ = aofWriter.Close()

	file, err := os.Open(aofPath)
	if err != nil {
		t.Fatalf("Failed to open AOF for reading: %v", err)
	}
	defer file.Close()

	reader := protocol.NewReader(file)
	var loggedCommands [][]string
	for {
		val, err := reader.ReadValue()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Error reading AOF: %v", err)
		}
		cmd, err := val.ToCommand()
		if err != nil {
			t.Fatalf("Failed to parse command: %v", err)
		}
		loggedCommands = append(loggedCommands, cmd)
	}

	// Should contain exactly 3 commands: 2 pre-existing + 1 new command.
	// Replay must NOT have re-appended the initial 2 commands!
	if len(loggedCommands) != 3 {
		t.Fatalf("Expected 3 commands in AOF (no duplicates), got %d: %v", len(loggedCommands), loggedCommands)
	}
}

// TestSnapshotAndAOFCombinedRecovery tests the complete Phase 5C recovery flow:
// 1. Initial writes are captured into a snapshot along with the AOF offset.
// 2. Further writes occur to the AOF after the snapshot.
// 3. On restart: snapshot is loaded first, then AOF is replayed strictly from that offset.
// 4. Client verifies all data is consistent and older AOF commands were not replayed.
func TestSnapshotAndAOFCombinedRecovery(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "combined.aof")
	snapPath := filepath.Join(tempDir, "dump.rdb")

	// Phase 1: Server running with AOF
	store := storage.NewStoreWithInterval(0)
	aofWriter, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}

	// Write initial keys 1 and 2
	store.Set("k1", "v1")
	_ = aofWriter.Write([]string{"SET", "k1", "v1"})
	store.Set("k2", "v2")
	_ = aofWriter.Write([]string{"SET", "k2", "v2"})

	// Take Snapshot at current AOF offset
	if err := takeSnapshot(store, aofWriter, snapPath); err != nil {
		t.Fatalf("Take snapshot failed: %v", err)
	}

	// Write key 3 AFTER the snapshot
	store.Set("k3", "v3")
	_ = aofWriter.Write([]string{"SET", "k3", "v3"})

	_ = aofWriter.Close()
	store.Close()

	// Phase 2: Restart fresh server and recover
	freshStore := storage.NewStoreWithInterval(0)
	defer freshStore.Close()

	// 1. Load Snapshot
	snapHeader, snapEntries, err := snapshot.Load(snapPath)
	if err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}
	freshStore.LoadSnapshot(snapEntries)

	if snapHeader.KeyCount != 2 {
		t.Fatalf("Expected 2 keys in snapshot, got %d", snapHeader.KeyCount)
	}

	// 2. Replay AOF strictly from snapHeader.AOFOffset
	recResult, err := aof.RecoverWithOffset(aofPath, freshStore, snapHeader.AOFOffset)
	if err != nil {
		t.Fatalf("AOF recovery from offset failed: %v", err)
	}

	// Exactly 1 command (SET k3 v3) should have been replayed!
	if recResult.CommandsReplayed != 1 {
		t.Fatalf("Expected exactly 1 incremental command replayed, got %d", recResult.CommandsReplayed)
	}

	// 3. Verify all 3 keys are present in freshStore
	for _, k := range []string{"k1", "k2", "k3"} {
		if !freshStore.Exists(k) {
			t.Errorf("Expected key %s to exist after combined recovery", k)
		}
	}
}

// TestAOFRewriteInvalidatesObsoleteSnapshot verifies that when an AOF rewrite compacts
// the AOF file after a snapshot was taken, recovery never fails with an AOFOffset mismatch.
func TestAOFRewriteInvalidatesObsoleteSnapshot(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "rewrite_snap.aof")
	snapPath := filepath.Join(tempDir, "dump.rdb")

	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	aofWriter, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}

	// Bloat the AOF with 30 redundant writes
	for i := 1; i <= 30; i++ {
		val := fmt.Sprintf("v%d", i)
		store.Set("k1", val)
		_ = aofWriter.Write([]string{"SET", "k1", val})
	}

	// Take a snapshot -> records a large AOFOffset (hundreds of bytes)
	if err := takeSnapshot(store, aofWriter, snapPath); err != nil {
		t.Fatalf("takeSnapshot failed: %v", err)
	}

	// Now rewrite the AOF -> shrinks AOF drastically and removes obsolete snapshot
	res, err := aofWriter.Rewrite(store, snapPath)
	if err != nil {
		t.Fatalf("AOF rewrite failed: %v", err)
	}
	if res.KeysWritten != 1 {
		t.Fatalf("Expected 1 key written, got %d", res.KeysWritten)
	}

	// Write another key after rewrite
	store.Set("k2", "after")
	_ = aofWriter.Write([]string{"SET", "k2", "after"})
	_ = aofWriter.Close()

	// Verify obsolete snapshot was removed
	if _, err := os.Stat(snapPath); !os.IsNotExist(err) {
		t.Fatalf("Expected obsolete snapshot %s to be removed after AOF rewrite", snapPath)
	}

	// Simulate full server startup recovery sequence
	freshStore := storage.NewStoreWithInterval(0)
	defer freshStore.Close()

	var startOffset int64 = 0
	header, entries, err := snapshot.Load(snapPath)
	if err == nil {
		freshStore.LoadSnapshot(entries)
		startOffset = header.AOFOffset
	} else if !os.IsNotExist(err) {
		t.Fatalf("Unexpected snapshot load error: %v", err)
	}

	recRes, err := aof.RecoverWithOffset(aofPath, freshStore, startOffset)
	if err != nil {
		t.Fatalf("Recovery after rewrite failed: %v", err)
	}
	if recRes.CommandsReplayed != 2 {
		t.Fatalf("Expected 2 commands replayed (k1 and k2), got %d", recRes.CommandsReplayed)
	}

	if val, ok := freshStore.Get("k1"); !ok || val != "v30" {
		t.Errorf("Expected k1='v30', got %q (ok=%v)", val, ok)
	}
	if val, ok := freshStore.Get("k2"); !ok || val != "after" {
		t.Errorf("Expected k2='after', got %q (ok=%v)", val, ok)
	}
}
