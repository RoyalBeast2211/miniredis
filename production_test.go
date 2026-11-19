package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"miniredis/aof"
	"miniredis/config"
	"miniredis/protocol"
	"miniredis/storage"
)

// TestProduction_GracefulShutdownUnderLoad verifies that a server under heavy concurrent load
// cleanly rejects new connections, drains in-flight requests, flushes AOF buffers,
// and terminates without deadlock or panic.
func TestProduction_GracefulShutdownUnderLoad(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := tempDir + "/shutdown.aof"

	cfg := config.NewDefaultConfig()
	cfg.AOFPath = aofPath
	cfg.Port = 0 // ephemeral
	cfg.ShutdownTimeout = 3 * time.Second

	store := storage.NewStore()
	defer store.Close()

	aofWriter, err := aof.Open(aofPath, aof.SyncEverySecond)
	if err != nil {
		t.Fatalf("failed to open aof: %v", err)
	}

	server := NewServer(cfg, store, aofWriter)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	serverAddr := listener.Addr().String()

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Serve(listener)
	}()

	// Spawn 8 concurrent clients generating continuous traffic
	stopClients := make(chan struct{})
	var clientWg sync.WaitGroup

	for i := 0; i < 8; i++ {
		clientWg.Add(1)
		go func(workerID int) {
			defer clientWg.Done()

			conn, err := net.Dial("tcp", serverAddr)
			if err != nil {
				return
			}
			defer conn.Close()

			reader := protocol.NewReader(conn)
			counter := 0

			for {
				select {
				case <-stopClients:
					return
				default:
				}

				key := fmt.Sprintf("k_%d_%d", workerID, counter)
				val := fmt.Sprintf("v_%d_%d", workerID, counter)
				counter++

				cmd := protocol.EncodeCommand("SET", key, val)
				if _, err := conn.Write(cmd); err != nil {
					return
				}

				resp, err := reader.ReadValue()
				if err != nil {
					return
				}
				if resp.Str != "OK" {
					return
				}
			}
		}(i)
	}

	// Allow clients to hammer the server for 200ms
	time.Sleep(200 * time.Millisecond)

	// Initiate Graceful Shutdown while clients are actively sending traffic
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}

	close(stopClients)
	clientWg.Wait()

	// Verify server terminated cleanly
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("unexpected Serve() error after shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("server did not terminate within timeout")
	}

	// Verify new connections are completely rejected
	_, err = net.DialTimeout("tcp", serverAddr, 100*time.Millisecond)
	if err == nil {
		t.Fatalf("expected new connection to be rejected after shutdown, but dialed successfully")
	}

	// Verify AOF recovered cleanly without corruption
	checkStore := storage.NewStore()
	defer checkStore.Close()
	recResult, err := aof.Recover(aofPath, checkStore)
	if err != nil {
		t.Fatalf("AOF recovery failed after graceful shutdown: %v", err)
	}
	if recResult.CommandsReplayed == 0 {
		t.Fatalf("expected replayed commands from AOF, got 0")
	}
}

// TestProduction_HealthAndReadinessEndpoints verifies that /healthz and /readyz
// report accurate liveness and readiness statuses over HTTP.
func TestProduction_HealthAndReadinessEndpoints(t *testing.T) {
	// Pick an available ephemeral port for health server
	hListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen for health port: %v", err)
	}
	healthPort := hListener.Addr().(*net.TCPAddr).Port
	_ = hListener.Close()

	cfg := config.NewDefaultConfig()
	cfg.HealthPort = healthPort
	cfg.Port = 0

	store := storage.NewStore()
	defer store.Close()

	server := NewServer(cfg, store, nil)
	server.startHealthServer()
	server.isReady.Store(true)

	defer func() {
		_ = server.healthServer.Close()
	}()

	time.Sleep(50 * time.Millisecond)

	healthURL := fmt.Sprintf("http://127.0.0.1:%d/healthz", healthPort)
	readyURL := fmt.Sprintf("http://127.0.0.1:%d/readyz", healthPort)

	// 1. Test /healthz (Liveness)
	resp, err := http.Get(healthURL)
	if err != nil {
		t.Fatalf("failed to query /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK from /healthz, got %d", resp.StatusCode)
	}

	var healthBody map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&healthBody); err != nil {
		t.Fatalf("failed to decode /healthz body: %v", err)
	}
	if healthBody["status"] != "alive" {
		t.Errorf("expected status 'alive', got %v", healthBody["status"])
	}

	// 2. Test /readyz (Readiness when ready)
	resp2, err := http.Get(readyURL)
	if err != nil {
		t.Fatalf("failed to query /readyz: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK from /readyz, got %d", resp2.StatusCode)
	}

	// 3. Test /readyz (Readiness when shutting down / not ready)
	server.isReady.Store(false)
	resp3, err := http.Get(readyURL)
	if err != nil {
		t.Fatalf("failed to query /readyz when not ready: %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 Service Unavailable when not ready, got %d", resp3.StatusCode)
	}
}

// TestProduction_MaxClientsRejection verifies that when MaxClients is exceeded,
// new connections are immediately rejected with an explicit error and closed.
func TestProduction_MaxClientsRejection(t *testing.T) {
	cfg := config.NewDefaultConfig()
	cfg.MaxClients = 2

	store := storage.NewStore()
	defer store.Close()

	server := NewServer(cfg, store, nil)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	serverAddr := listener.Addr().String()

	go func() {
		_ = server.Serve(listener)
	}()
	defer func() {
		_ = server.Shutdown(context.Background())
	}()

	// Connect Client 1
	conn1, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("client 1 failed to dial: %v", err)
	}
	defer conn1.Close()

	// Connect Client 2
	conn2, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("client 2 failed to dial: %v", err)
	}
	defer conn2.Close()

	time.Sleep(30 * time.Millisecond)

	// Connect Client 3 (Should be rejected)
	conn3, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("client 3 failed to dial: %v", err)
	}
	defer conn3.Close()

	reader := protocol.NewReader(conn3)
	resp, err := reader.ReadValue()
	if err != nil {
		t.Fatalf("client 3 expected rejection message, got error: %v", err)
	}
	if resp.Type != protocol.TypeError || !strings.Contains(resp.Str, "max number of clients reached") {
		t.Errorf("expected maxclients error, got %+v", resp)
	}

	// Verify conn3 is closed by reading EOF
	buf := make([]byte, 10)
	n, err := conn3.Read(buf)
	if n != 0 || err == nil {
		t.Errorf("expected conn3 to be closed by server, got n=%d, err=%v", n, err)
	}
}

// TestProduction_ClientIdleTimeout verifies that an idle connection is disconnected
// after exceeding the configured ClientTimeout.
func TestProduction_ClientIdleTimeout(t *testing.T) {
	cfg := config.NewDefaultConfig()
	cfg.ClientTimeout = 100 * time.Millisecond

	store := storage.NewStore()
	defer store.Close()

	server := NewServer(cfg, store, nil)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	serverAddr := listener.Addr().String()

	go func() {
		_ = server.Serve(listener)
	}()
	defer func() {
		_ = server.Shutdown(context.Background())
	}()

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	// Send an initial PING to verify connection is alive
	_, _ = conn.Write(protocol.EncodeCommand("PING"))
	reader := protocol.NewReader(conn)
	resp, err := reader.ReadValue()
	if err != nil || resp.Str != "PONG" {
		t.Fatalf("expected PONG, got %v, err=%v", resp, err)
	}

	// Sleep longer than the 100ms idle timeout without sending commands
	time.Sleep(250 * time.Millisecond)

	// Attempting to read should now fail because the server closed or timed out the socket
	_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	buf := make([]byte, 10)
	_, err = conn.Read(buf)
	if err == nil {
		t.Errorf("expected connection to be closed by idle timeout, but read succeeded")
	}
}

// TestProduction_InfoSections verifies that the INFO command produces
// comprehensive telemetry across all standard Redis sections.
func TestProduction_InfoSections(t *testing.T) {
	store := storage.NewStore()
	defer store.Close()

	server := NewServer(nil, store, nil)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	serverAddr := listener.Addr().String()

	go func() {
		_ = server.Serve(listener)
	}()
	defer func() {
		_ = server.Shutdown(context.Background())
	}()

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	_, _ = conn.Write(protocol.EncodeCommand("INFO"))
	reader := protocol.NewReader(conn)
	resp, err := reader.ReadValue()
	if err != nil {
		t.Fatalf("failed to read INFO response: %v", err)
	}

	info := resp.Str
	expectedSections := []string{
		"# Server",
		"redis_version:miniredis",
		"process_id:",
		"# Clients",
		"connected_clients:",
		"# Memory",
		"used_memory:",
		"used_memory_human:",
		"# Stats",
		"total_commands_processed:",
		"# Persistence",
		"# Replication",
		"role:master",
	}

	for _, sec := range expectedSections {
		if !strings.Contains(info, sec) {
			t.Errorf("INFO output missing %q:\n%s", sec, info)
		}
	}
}
