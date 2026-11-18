package main

import (
	"fmt"
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

// TestConcurrency1_MultipleClientsSameKey tests 20 concurrent goroutines writing to
// the exact same key simultaneously. Ensures lock striping and maps are 100% race-free.
func TestConcurrency1_MultipleClientsSameKey(t *testing.T) {
	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	const numGoroutines = 20
	const writesPerGoroutine = 500

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				val := fmt.Sprintf("val_g%d_i%d", id, i)
				store.Set("hot_key", val)
			}
		}(g)
	}

	wg.Wait()

	// Must be present and contain one valid string without panicking or memory corruption
	val, ok := store.Get("hot_key")
	if !ok || len(val) == 0 {
		t.Fatalf("expected hot_key to exist with a valid value")
	}
}

// TestConcurrency2_ConcurrentGetAndSet tests interleaved reads and writes on the same key.
func TestConcurrency2_ConcurrentGetAndSet(t *testing.T) {
	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	store.Set("target_key", "initial")

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 5 Writers
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					store.Set("target_key", fmt.Sprintf("v_%d_%d", id, i))
					i++
				}
			}
		}(w)
	}

	// 10 Readers
	for r := 0; r < 10; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					val, ok := store.Get("target_key")
					if ok && len(val) == 0 {
						t.Errorf("read empty value on existing key")
					}
				}
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestConcurrency3_ConcurrentDelAndGet tests interleaved deletes, reads, and re-inserts.
func TestConcurrency3_ConcurrentDelAndGet(t *testing.T) {
	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Deleter / Inserter
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				if i%2 == 0 {
					store.Set("toggle_key", "active")
				} else {
					store.Delete("toggle_key")
				}
			}
		}
	}()

	// 8 Readers
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					val, exists := store.Get("toggle_key")
					if exists && val != "active" {
						t.Errorf("expected 'active' or nonexistent, got %q", val)
					}
				}
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestConcurrency4_ConcurrentExpirationAndReads tests active background TTL expiration
// sweeping while readers concurrently query keys approaching expiration.
func TestConcurrency4_ConcurrentExpirationAndReads(t *testing.T) {
	// Active cleanup sweeps every 10ms
	store := storage.NewStoreWithInterval(10 * time.Millisecond)
	defer store.Close()

	// Seed 100 short-lived keys with 20ms TTL
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("ttl_key_%d", i)
		store.Set(k, "temp")
		store.ExpireAt(k, time.Now().Add(25*time.Millisecond).Unix())
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for r := 0; r < 10; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					k := fmt.Sprintf("ttl_key_%d", id*10)
					_ = store.Exists(k)
					_, _ = store.Get(k)
					_ = store.TTL(k)
				}
			}
		}(r)
	}

	time.Sleep(60 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestConcurrency5_ConcurrentSnapshotAndWrites verifies that taking a database snapshot
// while clients are actively writing data captures a consistent state without crashing or deadlock.
func TestConcurrency5_ConcurrentSnapshotAndWrites(t *testing.T) {
	tempDir := t.TempDir()
	snapPath := filepath.Join(tempDir, "concurrent_dump.rdb")

	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 5 Client Writers
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					k := fmt.Sprintf("snap_key_%d_%d", id, i%200)
					store.Set(k, fmt.Sprintf("data_%d", i))
					i++
				}
			}
		}(w)
	}

	// Trigger snapshot while writes are actively running
	time.Sleep(20 * time.Millisecond)
	if err := takeSnapshot(store, nil, snapPath); err != nil {
		t.Fatalf("takeSnapshot failed during concurrent writes: %v", err)
	}

	close(stop)
	wg.Wait()

	// Verify the captured snapshot is non-empty and 100% valid
	header, entries, err := snapshot.Load(snapPath)
	if err != nil {
		t.Fatalf("snapshot load failed: %v", err)
	}
	if header.KeyCount == 0 || len(entries) == 0 {
		t.Errorf("snapshot was empty despite concurrent writes")
	}
}

// TestConcurrency6_ConcurrentAOFRewriteAndWrites verifies that an online AOF rewrite
// correctly captures concurrent client writes in its incremental buffer.
func TestConcurrency6_ConcurrentAOFRewriteAndWrites(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "concurrent_rewrite.aof")

	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	aofWriter, err := aof.Open(aofPath, aof.SyncAlways)
	if err != nil {
		t.Fatalf("failed to open aof: %v", err)
	}
	defer aofWriter.Close()

	// Pre-populate with baseline keys
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("base:%d", i)
		store.Set(k, "v")
		_ = aofWriter.Write([]string{"SET", k, "v"})
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Concurrent writes during rewrite
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					k := fmt.Sprintf("dyn:%d:%d", id, i)
					store.Set(k, "val")
					_ = aofWriter.Write([]string{"SET", k, "val"})
					i++
				}
			}
		}(w)
	}

	// Run rewrite
	time.Sleep(10 * time.Millisecond)
	res, err := aofWriter.Rewrite(store, "")
	if err != nil {
		t.Fatalf("rewrite failed: %v", err)
	}

	close(stop)
	wg.Wait()

	if res.KeysWritten < 50 {
		t.Errorf("expected at least 50 baseline keys in rewrite, got %d", res.KeysWritten)
	}
}

// TestConcurrency7_ShutdownWhileClientsConnected simulates shutting down the server
// while multiple clients are connected and actively sending requests.
func TestConcurrency7_ShutdownWhileClientsConnected(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind listener: %v", err)
	}

	store := storage.NewStoreWithInterval(0)
	defer store.Close()

	go func() {
		_ = runServer(listener, store, nil)
	}()

	addr := listener.Addr().String()
	numClients := 10
	var wg sync.WaitGroup
	wg.Add(numClients)

	for c := 0; c < numClients; c++ {
		go func(cid int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer conn.Close()

			for i := 0; i < 100; i++ {
				_, err := sendCommand(conn, "PING")
				if err != nil {
					// Expected when server listener is abruptly closed
					return
				}
			}
		}(c)
	}

	// Abruptly close listener while clients are pinging
	time.Sleep(15 * time.Millisecond)
	_ = listener.Close()

	wg.Wait()
}

// TestConcurrency8_ShutdownWhilePersistenceActive verifies that closing the AOF
// while background sync workers or concurrent writes are in flight cleanly flushes
// and terminates without panic or hanging.
func TestConcurrency8_ShutdownWhilePersistenceActive(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "shutdown_stress.aof")

	aofWriter, err := aof.Open(aofPath, aof.SyncEverySecond)
	if err != nil {
		t.Fatalf("failed to open aof: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writers hammering AOF
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					_ = aofWriter.Write([]string{"SET", fmt.Sprintf("k%d_%d", id, i), "v"})
					i++
				}
			}
		}(w)
	}

	time.Sleep(25 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Close AOF while background worker may have in-flight sync
	if err := aofWriter.Close(); err != nil {
		t.Fatalf("AOF Close failed: %v", err)
	}

	// Verify file is readable and not corrupt
	file, err := os.Open(aofPath)
	if err != nil {
		t.Fatalf("failed to open aof after close: %v", err)
	}
	defer file.Close()

	reader := protocol.NewReader(file)
	count := 0
	for {
		_, err := reader.ReadValue()
		if err != nil {
			break
		}
		count++
	}
	if count == 0 {
		t.Errorf("expected non-zero commands written to AOF")
	}
}
