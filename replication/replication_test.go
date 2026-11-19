package replication

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"miniredis/protocol"
	"miniredis/storage"
)

// startTestPrimaryServer starts a TCP server running the primary replication protocol.
func startTestPrimaryServer(t *testing.T, store *storage.Store, backlogCapacity int64) (net.Listener, *Primary) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start test primary listener: %v", err)
	}

	primary := NewPrimary(store, backlogCapacity)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleTestPrimaryConn(primary, conn)
		}
	}()

	return listener, primary
}

// handleTestPrimaryConn simulates primary connection handling.
func handleTestPrimaryConn(primary *Primary, conn net.Conn) {
	defer conn.Close()

	reader := protocol.NewReader(conn)
	var activePeer *ReplicaPeer

	defer func() {
		if activePeer != nil {
			primary.RemovePeer(activePeer.id)
			activePeer.Close()
		}
	}()

	for {
		val, err := reader.ReadValue()
		if err != nil {
			return
		}

		args, err := val.ToCommand()
		if err != nil {
			continue
		}
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])
		switch cmd {
		case "PING":
			_, _ = conn.Write([]byte("+PONG\r\n"))

		case "REPLCONF":
			resp := primary.HandleReplConf(conn.RemoteAddr().String(), args)
			if resp.Type != 0 {
				_, _ = conn.Write(resp.Encode())
			}

		case "PSYNC":
			if len(args) < 3 {
				_, _ = conn.Write(protocol.NewError("ERR wrong number of arguments for psync").Encode())
				continue
			}
			offset, _ := strconv.ParseInt(args[2], 10, 64)
			peer, err := primary.HandlePSync(conn, args[1], offset)
			if err != nil {
				return
			}
			activePeer = peer

		default:
			_, _ = conn.Write(protocol.NewError(fmt.Sprintf("ERR unknown command '%s'", cmd)).Encode())
		}
	}
}

// TestReplication_HandshakeAndFullSync verifies that a newly connecting replica
// receives the point-in-time snapshot and populates its local storage engine.
func TestReplication_HandshakeAndFullSync(t *testing.T) {
	pStore := storage.NewStore()
	defer pStore.Close()

	pStore.Set("user:1", "Alice")
	pStore.Set("user:2", "Bob")

	listener, primary := startTestPrimaryServer(t, pStore, 1024*1024)
	defer listener.Close()
	defer primary.Close()

	rStore := storage.NewStore()
	defer rStore.Close()

	replica := NewReplica(listener.Addr().String(), "6380", rStore)
	replica.SetIntervals(50*time.Millisecond, 50*time.Millisecond)
	replica.Start()
	defer replica.Close()

	if err := replica.WaitForInitialSync(2 * time.Second); err != nil {
		t.Fatalf("WaitForInitialSync failed: %v", err)
	}

	val, exists := rStore.Get("user:1")
	if !exists || val != "Alice" {
		t.Errorf("expected user:1='Alice', got %q, exists=%v", val, exists)
	}

	val, exists = rStore.Get("user:2")
	if !exists || val != "Bob" {
		t.Errorf("expected user:2='Bob', got %q, exists=%v", val, exists)
	}
}

// TestReplication_IncrementalStreaming verifies that subsequent write mutations on the primary
// are streamed over TCP and applied to the replica in real-time.
func TestReplication_IncrementalStreaming(t *testing.T) {
	pStore := storage.NewStore()
	defer pStore.Close()

	listener, primary := startTestPrimaryServer(t, pStore, 1024*1024)
	defer listener.Close()
	defer primary.Close()

	rStore := storage.NewStore()
	defer rStore.Close()

	replica := NewReplica(listener.Addr().String(), "6380", rStore)
	replica.SetIntervals(50*time.Millisecond, 50*time.Millisecond)
	replica.Start()
	defer replica.Close()

	if err := replica.WaitForInitialSync(2 * time.Second); err != nil {
		t.Fatalf("WaitForInitialSync failed: %v", err)
	}

	// 1. Propagate SET
	pStore.Set("msg", "hello")
	primary.Propagate([]string{"SET", "msg", "hello"})

	// 2. Propagate EXPIREAT
	futureSec := time.Now().Add(5 * time.Minute).Unix()
	pStore.ExpireAt("msg", futureSec)
	primary.Propagate([]string{"EXPIREAT", "msg", strconv.FormatInt(futureSec, 10)})

	// Wait for async stream propagation
	time.Sleep(100 * time.Millisecond)

	val, exists := rStore.Get("msg")
	if !exists || val != "hello" {
		t.Fatalf("expected msg='hello' on replica, got %q, exists=%v", val, exists)
	}
	ttl := rStore.TTL("msg")
	if ttl <= 0 {
		t.Fatalf("expected positive TTL on replica, got %d", ttl)
	}

	// 3. Propagate DEL
	pStore.Delete("msg")
	primary.Propagate([]string{"DEL", "msg"})

	time.Sleep(100 * time.Millisecond)

	_, exists = rStore.Get("msg")
	if exists {
		t.Fatalf("expected msg to be deleted on replica")
	}

	// Verify replication offsets match
	if replica.AppliedOffset() != primary.CurrentOffset() {
		t.Errorf("offset mismatch: replica=%d, primary=%d", replica.AppliedOffset(), primary.CurrentOffset())
	}
}

// TestReplication_PartialResynchronization verifies that when a replica temporarily disconnects
// and reconnects with its last acknowledged offset, the primary performs a partial resync (+CONTINUE).
func TestReplication_PartialResynchronization(t *testing.T) {
	pStore := storage.NewStore()
	defer pStore.Close()

	listener, primary := startTestPrimaryServer(t, pStore, 1024*1024)
	defer listener.Close()
	defer primary.Close()

	rStore := storage.NewStore()
	defer rStore.Close()

	replica := NewReplica(listener.Addr().String(), "6380", rStore)
	replica.SetIntervals(50*time.Millisecond, 50*time.Millisecond)
	replica.Start()

	if err := replica.WaitForInitialSync(2 * time.Second); err != nil {
		t.Fatalf("initial sync failed: %v", err)
	}

	// Write key 1
	pStore.Set("k1", "v1")
	primary.Propagate([]string{"SET", "k1", "v1"})
	time.Sleep(100 * time.Millisecond)

	savedReplID := replica.ReplID()
	savedOffset := replica.AppliedOffset()

	// Disconnect replica
	replica.Close()
	time.Sleep(50 * time.Millisecond)

	// Primary writes while replica is offline
	pStore.Set("k2", "v2")
	primary.Propagate([]string{"SET", "k2", "v2"})
	pStore.Set("k3", "v3")
	primary.Propagate([]string{"SET", "k3", "v3"})

	// Reconnect new replica instance simulating restart with saved state
	replica2 := NewReplica(listener.Addr().String(), "6380", rStore)
	replica2.SetIntervals(50*time.Millisecond, 50*time.Millisecond)
	// Initialize with saved state
	replica2.mu.Lock()
	replica2.replID = savedReplID
	replica2.appliedOffset = savedOffset
	replica2.mu.Unlock()

	replica2.Start()
	defer replica2.Close()

	if err := replica2.WaitForInitialSync(2 * time.Second); err != nil {
		t.Fatalf("reconnect sync failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Verify all keys are present
	for _, k := range []string{"k1", "k2", "k3"} {
		if _, exists := rStore.Get(k); !exists {
			t.Errorf("expected key %s to exist on replica after partial resync", k)
		}
	}

	if replica2.AppliedOffset() != primary.CurrentOffset() {
		t.Errorf("offset mismatch after partial resync: replica=%d, primary=%d",
			replica2.AppliedOffset(), primary.CurrentOffset())
	}
}

// TestReplication_FullResyncOnBacklogOverflow verifies that if a disconnected replica's offset
// has been evicted from the primary's circular backlog, the primary forces a Full Resync (+FULLRESYNC).
func TestReplication_FullResyncOnBacklogOverflow(t *testing.T) {
	pStore := storage.NewStore()
	defer pStore.Close()

	// Tiny backlog of only 64 bytes
	listener, primary := startTestPrimaryServer(t, pStore, 64)
	defer listener.Close()
	defer primary.Close()

	rStore := storage.NewStore()
	defer rStore.Close()

	replica := NewReplica(listener.Addr().String(), "6380", rStore)
	replica.SetIntervals(50*time.Millisecond, 50*time.Millisecond)
	replica.Start()

	if err := replica.WaitForInitialSync(2 * time.Second); err != nil {
		t.Fatalf("initial sync failed: %v", err)
	}

	savedReplID := replica.ReplID()
	savedOffset := replica.AppliedOffset()

	replica.Close()
	time.Sleep(50 * time.Millisecond)

	// Write enough data to completely overwrite the 64-byte backlog
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("overflow_key_%d", i)
		v := fmt.Sprintf("overflow_val_%d", i)
		pStore.Set(k, v)
		primary.Propagate([]string{"SET", k, v})
	}

	// Verify that savedOffset is now older than primary's firstOffset
	if primary.Backlog().CanPartialResync(savedReplID, savedOffset) {
		t.Fatalf("expected offset %d to be evicted from backlog", savedOffset)
	}

	// Reconnect replica with old offset
	replica2 := NewReplica(listener.Addr().String(), "6380", rStore)
	replica2.SetIntervals(50*time.Millisecond, 50*time.Millisecond)
	replica2.mu.Lock()
	replica2.replID = savedReplID
	replica2.appliedOffset = savedOffset
	replica2.mu.Unlock()

	replica2.Start()
	defer replica2.Close()

	if err := replica2.WaitForInitialSync(2 * time.Second); err != nil {
		t.Fatalf("reconnect full resync failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Verify all keys from overflow writes exist in replica
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("overflow_key_%d", i)
		if _, exists := rStore.Get(k); !exists {
			t.Errorf("missing %s on replica after full resync", k)
		}
	}
}

// TestReplication_MultipleReplicas verifies that a single primary successfully broadcasts
// write streams to multiple replicas concurrently.
func TestReplication_MultipleReplicas(t *testing.T) {
	pStore := storage.NewStore()
	defer pStore.Close()

	listener, primary := startTestPrimaryServer(t, pStore, 1024*1024)
	defer listener.Close()
	defer primary.Close()

	numReplicas := 3
	replicas := make([]*Replica, numReplicas)
	stores := make([]*storage.Store, numReplicas)

	var wg sync.WaitGroup
	for i := 0; i < numReplicas; i++ {
		stores[i] = storage.NewStore()
		defer stores[i].Close()

		port := fmt.Sprintf("%d", 6381+i)
		replicas[i] = NewReplica(listener.Addr().String(), port, stores[i])
		replicas[i].SetIntervals(30*time.Millisecond, 30*time.Millisecond)
		replicas[i].Start()
		defer replicas[i].Close()

		wg.Add(1)
		go func(r *Replica) {
			defer wg.Done()
			_ = r.WaitForInitialSync(2 * time.Second)
		}(replicas[i])
	}

	wg.Wait()

	// Propagate 50 writes
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("broadcast_%d", i)
		val := fmt.Sprintf("data_%d", i)
		pStore.Set(key, val)
		primary.Propagate([]string{"SET", key, val})
	}

	// Give replicas time to catch up
	time.Sleep(150 * time.Millisecond)

	// Verify all replicas received all 50 keys
	for rIdx, store := range stores {
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("broadcast_%d", i)
			expectedVal := fmt.Sprintf("data_%d", i)
			gotVal, exists := store.Get(key)
			if !exists || gotVal != expectedVal {
				t.Errorf("replica %d missing key %s: got %q, exists=%v", rIdx, key, gotVal, exists)
			}
		}
	}
}
