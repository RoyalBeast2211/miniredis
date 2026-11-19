package main

import (
	"net"
	"strings"
	"testing"
	"time"

	"miniredis/protocol"
	"miniredis/storage"
)

// startTestServerWithContext starts an in-process TCP server with the given context.
func startTestServerWithContext(t *testing.T, ctx *ServerContext) (net.Listener, string) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on ephemeral port: %v", err)
	}

	go func() {
		_ = runServerWithContext(listener, ctx)
	}()

	return listener, listener.Addr().String()
}

// sendClientCommand sends a RESP command over a TCP connection and returns the parsed response.
func sendClientCommand(t *testing.T, conn net.Conn, args ...string) protocol.Value {
	req := protocol.EncodeCommand(args...)
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("failed to write command %v: %v", args, err)
	}

	reader := protocol.NewReader(conn)
	resp, err := reader.ReadValue()
	if err != nil {
		t.Fatalf("failed to read response for %v: %v", args, err)
	}
	return resp
}

// TestServer_ReadOnlyReplicaRejection verifies that a server configured as a replica
// strictly rejects all write commands with the standard Redis READONLY error,
// while permitting read and utility commands.
func TestServer_ReadOnlyReplicaRejection(t *testing.T) {
	store := storage.NewStore()
	defer store.Close()

	// Initial key in store
	store.Set("existing", "stored_value")

	ctx := NewServerContext(store, nil, true, "127.0.0.1:9999", "6380")
	defer ctx.Close()

	listener, addr := startTestServerWithContext(t, ctx)
	defer listener.Close()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial test replica: %v", err)
	}
	defer conn.Close()

	// 1. Verify write commands are rejected
	writeTests := [][]string{
		{"SET", "k", "v"},
		{"DEL", "existing"},
		{"EXPIRE", "existing", "60"},
		{"EXPIREAT", "existing", "1999999999"},
		{"PERSIST", "existing"},
	}

	for _, cmd := range writeTests {
		resp := sendClientCommand(t, conn, cmd...)
		if resp.Type != protocol.TypeError {
			t.Errorf("command %s: expected error response, got type %v", cmd[0], resp.Type)
		}
		if !strings.Contains(resp.Str, "READONLY") {
			t.Errorf("command %s: expected READONLY error, got %q", cmd[0], resp.Str)
		}
	}

	// 2. Verify read commands succeed
	getResp := sendClientCommand(t, conn, "GET", "existing")
	if getResp.Type != protocol.TypeBulkString || getResp.Str != "stored_value" {
		t.Errorf("expected GET to return 'stored_value', got %+v", getResp)
	}

	pingResp := sendClientCommand(t, conn, "PING")
	if pingResp.Type != protocol.TypeSimpleString || pingResp.Str != "PONG" {
		t.Errorf("expected PING to return 'PONG', got %+v", pingResp)
	}

	echoResp := sendClientCommand(t, conn, "ECHO", "hello replica")
	if echoResp.Type != protocol.TypeBulkString || echoResp.Str != "hello replica" {
		t.Errorf("expected ECHO to return 'hello replica', got %+v", echoResp)
	}
}

// TestServer_RoleAndInfoCommands verifies that ROLE and INFO commands
// return accurate cluster metadata for both primary and replica nodes.
func TestServer_RoleAndInfoCommands(t *testing.T) {
	// Primary
	pStore := storage.NewStore()
	defer pStore.Close()
	pCtx := NewServerContext(pStore, nil, false, "", "6379")
	defer pCtx.Close()

	pListener, pAddr := startTestServerWithContext(t, pCtx)
	defer pListener.Close()

	pConn, err := net.Dial("tcp", pAddr)
	if err != nil {
		t.Fatalf("failed to dial primary: %v", err)
	}
	defer pConn.Close()

	roleResp := sendClientCommand(t, pConn, "ROLE")
	if roleResp.Type != protocol.TypeArray || len(roleResp.Array) < 3 {
		t.Fatalf("expected ROLE array from primary, got %+v", roleResp)
	}
	if roleResp.Array[0].Str != "master" {
		t.Errorf("expected master role, got %s", roleResp.Array[0].Str)
	}

	infoResp := sendClientCommand(t, pConn, "INFO", "replication")
	if !strings.Contains(infoResp.Str, "role:master") {
		t.Errorf("expected INFO to contain 'role:master', got %q", infoResp.Str)
	}

	// Replica
	rStore := storage.NewStore()
	defer rStore.Close()
	rCtx := NewServerContext(rStore, nil, true, pAddr, "6380")
	defer rCtx.Close()

	rListener, rAddr := startTestServerWithContext(t, rCtx)
	defer rListener.Close()

	rConn, err := net.Dial("tcp", rAddr)
	if err != nil {
		t.Fatalf("failed to dial replica: %v", err)
	}
	defer rConn.Close()

	rRoleResp := sendClientCommand(t, rConn, "ROLE")
	if rRoleResp.Type != protocol.TypeArray || len(rRoleResp.Array) < 5 {
		t.Fatalf("expected ROLE array from replica, got %+v", rRoleResp)
	}
	if rRoleResp.Array[0].Str != "slave" {
		t.Errorf("expected slave role, got %s", rRoleResp.Array[0].Str)
	}

	rInfoResp := sendClientCommand(t, rConn, "INFO", "replication")
	if !strings.Contains(rInfoResp.Str, "role:slave") {
		t.Errorf("expected INFO to contain 'role:slave', got %q", rInfoResp.Str)
	}
}

// TestServer_EndToEndReplication verifies complete end-to-end client replication:
// initial snapshot loading, continuous write streaming, key deletion, and TTL synchronization.
func TestServer_EndToEndReplication(t *testing.T) {
	// 1. Setup Primary
	pStore := storage.NewStore()
	defer pStore.Close()
	pCtx := NewServerContext(pStore, nil, false, "", "6379")
	defer pCtx.Close()

	pListener, pAddr := startTestServerWithContext(t, pCtx)
	defer pListener.Close()

	pConn, err := net.Dial("tcp", pAddr)
	if err != nil {
		t.Fatalf("failed to dial primary: %v", err)
	}
	defer pConn.Close()

	// Seed data on primary before replica starts
	sendClientCommand(t, pConn, "SET", "seed_key", "seed_val")

	// 2. Setup Replica pointing to Primary
	rStore := storage.NewStore()
	defer rStore.Close()
	rCtx := NewServerContext(rStore, nil, true, pAddr, "6380")
	defer rCtx.Close()

	rListener, rAddr := startTestServerWithContext(t, rCtx)
	defer rListener.Close()

	rConn, err := net.Dial("tcp", rAddr)
	if err != nil {
		t.Fatalf("failed to dial replica: %v", err)
	}
	defer rConn.Close()

	// Wait for initial sync to complete
	if err := rCtx.replica.WaitForInitialSync(3 * time.Second); err != nil {
		t.Fatalf("WaitForInitialSync failed: %v", err)
	}

	// Verify pre-existing seed key exists on replica
	seedResp := sendClientCommand(t, rConn, "GET", "seed_key")
	if seedResp.Type != protocol.TypeBulkString || seedResp.Str != "seed_val" {
		t.Fatalf("expected seed_key='seed_val' on replica, got %+v", seedResp)
	}

	// 3. Write live commands to Primary
	setResp := sendClientCommand(t, pConn, "SET", "live_key", "live_val")
	if setResp.Str != "OK" {
		t.Fatalf("expected OK from primary SET, got %s", setResp.Str)
	}

	// Propagate TTL
	expResp := sendClientCommand(t, pConn, "EXPIRE", "live_key", "300")
	if expResp.Num != 1 {
		t.Fatalf("expected 1 from primary EXPIRE, got %d", expResp.Num)
	}

	// Give async replication stream a moment to arrive
	time.Sleep(100 * time.Millisecond)

	// Verify live key and TTL on replica
	liveResp := sendClientCommand(t, rConn, "GET", "live_key")
	if liveResp.Type != protocol.TypeBulkString || liveResp.Str != "live_val" {
		t.Fatalf("expected live_key='live_val' on replica, got %+v", liveResp)
	}

	ttlResp := sendClientCommand(t, rConn, "TTL", "live_key")
	if ttlResp.Num <= 0 {
		t.Fatalf("expected positive TTL on replica, got %d", ttlResp.Num)
	}

	// 4. Delete on primary
	delResp := sendClientCommand(t, pConn, "DEL", "seed_key")
	if delResp.Num != 1 {
		t.Fatalf("expected 1 from primary DEL, got %d", delResp.Num)
	}

	time.Sleep(100 * time.Millisecond)

	existsResp := sendClientCommand(t, rConn, "EXISTS", "seed_key")
	if existsResp.Num != 0 {
		t.Fatalf("expected seed_key to be deleted on replica, got exists=%d", existsResp.Num)
	}
}

// TestServer_DynamicReplicaOfPromotion verifies that sending REPLICAOF NO ONE
// promotes a replica node into an independent primary that can accept writes.
func TestServer_DynamicReplicaOfPromotion(t *testing.T) {
	// Setup Primary
	pStore := storage.NewStore()
	defer pStore.Close()
	pCtx := NewServerContext(pStore, nil, false, "", "6379")
	defer pCtx.Close()

	pListener, pAddr := startTestServerWithContext(t, pCtx)
	defer pListener.Close()

	// Setup Replica
	rStore := storage.NewStore()
	defer rStore.Close()
	rCtx := NewServerContext(rStore, nil, true, pAddr, "6380")
	defer rCtx.Close()

	rListener, rAddr := startTestServerWithContext(t, rCtx)
	defer rListener.Close()

	rConn, err := net.Dial("tcp", rAddr)
	if err != nil {
		t.Fatalf("failed to dial replica: %v", err)
	}
	defer rConn.Close()

	if err := rCtx.replica.WaitForInitialSync(3 * time.Second); err != nil {
		t.Fatalf("WaitForInitialSync failed: %v", err)
	}

	// Verify write is rejected before promotion
	rejResp := sendClientCommand(t, rConn, "SET", "fail_key", "v")
	if !strings.Contains(rejResp.Str, "READONLY") {
		t.Fatalf("expected READONLY error, got %+v", rejResp)
	}

	// Promote replica using REPLICAOF NO ONE
	promResp := sendClientCommand(t, rConn, "REPLICAOF", "NO", "ONE")
	if promResp.Type != protocol.TypeSimpleString || promResp.Str != "OK" {
		t.Fatalf("expected OK from REPLICAOF NO ONE, got %+v", promResp)
	}

	// Verify ROLE is now master
	roleResp := sendClientCommand(t, rConn, "ROLE")
	if roleResp.Array[0].Str != "master" {
		t.Fatalf("expected role master after promotion, got %s", roleResp.Array[0].Str)
	}

	// Verify write commands now succeed on the promoted node
	writeResp := sendClientCommand(t, rConn, "SET", "promoted_key", "success")
	if writeResp.Type != protocol.TypeSimpleString || writeResp.Str != "OK" {
		t.Fatalf("expected OK from SET on promoted primary, got %+v", writeResp)
	}

	readResp := sendClientCommand(t, rConn, "GET", "promoted_key")
	if readResp.Str != "success" {
		t.Fatalf("expected 'success', got %q", readResp.Str)
	}
}
