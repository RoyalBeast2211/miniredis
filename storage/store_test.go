package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestStoreBasicOperations verifies standard CRUD operations on the Store.
func TestStoreBasicOperations(t *testing.T) {
	store := NewStoreWithInterval(0) // Disable background cleanup to test deterministic unit logic
	defer store.Close()

	// 1. Retrieving a missing key
	val, exists := store.Get("missingKey")
	if exists || val != "" {
		t.Fatalf("Expected missingKey to not exist, got val=%q, exists=%v", val, exists)
	}

	// 2. Checking existence of missing key
	if store.Exists("missingKey") {
		t.Fatalf("Expected Exists('missingKey') to be false")
	}

	// 3. Inserting a new key
	store.Set("name", "Alice")

	// 4. Retrieving an existing key
	val, exists = store.Get("name")
	if !exists || val != "Alice" {
		t.Fatalf("Expected 'Alice', got val=%q, exists=%v", val, exists)
	}

	// 5. Checking existence of present key
	if !store.Exists("name") {
		t.Fatalf("Expected Exists('name') to be true")
	}

	// 6. Updating an existing key
	store.Set("name", "Bob")
	val, exists = store.Get("name")
	if !exists || val != "Bob" {
		t.Fatalf("Expected 'Bob' after update, got val=%q, exists=%v", val, exists)
	}

	// 7. Deleting an existing key
	deleted := store.Delete("name")
	if !deleted {
		t.Fatalf("Expected Delete('name') to return true")
	}

	// Verify it's gone
	if store.Exists("name") {
		t.Fatalf("Expected key 'name' to be gone after deletion")
	}

	// 8. Deleting a key that does not exist
	deletedAgain := store.Delete("name")
	if deletedAgain {
		t.Fatalf("Expected Delete('name') to return false when key is absent")
	}
}

// TestLRUEviction verifies LRU eviction policies, capacity limits, and recency updates.
func TestLRUEviction(t *testing.T) {
	// Capacity = 3, Active cleanup disabled for deterministic tests
	store := NewStoreWithConfig(3, 0)
	defer store.Close()

	// 1. Inserting keys below capacity
	store.Set("k1", "v1")
	store.Set("k2", "v2")
	store.Set("k3", "v3")

	// Order: MRU -> k3, k2, k1 -> LRU
	verifyStoreConsistency(t, store, 3)

	// 2. Inserting key when capacity is full -> should evict LRU (k1)
	store.Set("k4", "v4")
	// Order: MRU -> k4, k3, k2 -> LRU
	verifyStoreConsistency(t, store, 3)

	if store.Exists("k1") {
		t.Fatalf("Expected k1 to be evicted")
	}
	if !store.Exists("k2") || !store.Exists("k3") || !store.Exists("k4") {
		t.Fatalf("Expected k2, k3, and k4 to remain in store")
	}

	// 3. Accessing a key via GET changes its recency
	// Access k2: Order becomes: MRU -> k2, k4, k3 -> LRU
	val, exists := store.Get("k2")
	if !exists || val != "v2" {
		t.Fatalf("Expected to get k2")
	}

	// Now insert k5 -> since k3 is now LRU, k3 must be evicted, NOT k2!
	store.Set("k5", "v5")
	// Order: MRU -> k5, k2, k4 -> LRU
	verifyStoreConsistency(t, store, 3)

	if store.Exists("k3") {
		t.Fatalf("Expected k3 to be evicted as least recently used")
	}
	if !store.Exists("k2") {
		t.Fatalf("Expected k2 to still exist because it was recently accessed")
	}

	// 4. Updating an existing key moves it to MRU without increasing count
	store.Set("k4", "v4_updated")
	// Order: MRU -> k4, k5, k2 -> LRU
	verifyStoreConsistency(t, store, 3)

	// Insert k6 -> should evict k2 (LRU)
	store.Set("k6", "v6")
	// Order: MRU -> k6, k4, k5 -> LRU
	verifyStoreConsistency(t, store, 3)

	if store.Exists("k2") {
		t.Fatalf("Expected k2 to be evicted")
	}
	if !store.Exists("k4") {
		t.Fatalf("Expected k4 to be retained because update moved it to MRU")
	}

	// 5. Deleting a key removes it from LRU list and creates space
	store.Delete("k4")
	verifyStoreConsistency(t, store, 2)

	// Inserting k7 should now not evict anything because size was 2 < capacity 3
	store.Set("k7", "v7")
	verifyStoreConsistency(t, store, 3)
	if !store.Exists("k5") || !store.Exists("k6") || !store.Exists("k7") {
		t.Fatalf("Expected k5, k6, and k7 to all be present")
	}
}

// TestLRUCapacityOne tests the boundary condition of capacity = 1.
func TestLRUCapacityOne(t *testing.T) {
	store := NewStoreWithConfig(1, 0)
	defer store.Close()

	store.Set("a", "1")
	verifyStoreConsistency(t, store, 1)

	store.Set("b", "2")
	verifyStoreConsistency(t, store, 1)

	if store.Exists("a") {
		t.Fatalf("Expected 'a' to be evicted in capacity 1 store")
	}
	if val, ok := store.Get("b"); !ok || val != "2" {
		t.Fatalf("Expected 'b' to be present")
	}
}

// TestLRUTTLIntegration verifies that expired keys are purged before evicting valid keys.
func TestLRUTTLIntegration(t *testing.T) {
	// Capacity = 2
	store := NewStoreWithConfig(2, 0)
	defer store.Close()

	store.Set("validKey", "data1")
	store.Set("expiringKey", "data2")
	store.Expire("expiringKey", 1) // 1 second TTL

	// Wait 1.1 seconds so expiringKey expires
	time.Sleep(1100 * time.Millisecond)

	// Now insert newKey3.
	// Capacity is 2, and data map currently has 2 items.
	// However, expiringKey has passed its deadline!
	// Set() should clean up expiringKey first, preserving validKey!
	store.Set("newKey3", "data3")

	verifyStoreConsistency(t, store, 2)

	if !store.Exists("validKey") {
		t.Fatalf("Expected validKey to be preserved because expiringKey was cleaned up first")
	}
	if !store.Exists("newKey3") {
		t.Fatalf("Expected newKey3 to exist")
	}
	if store.Exists("expiringKey") {
		t.Fatalf("Expected expiringKey to be gone")
	}
}

// TestTTLExpiration verifies lazy expiration, TTL values, and PERSIST semantics.
func TestTTLExpiration(t *testing.T) {
	store := NewStoreWithInterval(0)
	defer store.Close()

	if ttl := store.TTL("nonexistent"); ttl != -2 {
		t.Fatalf("Expected TTL on nonexistent key to be -2, got %d", ttl)
	}

	store.Set("mykey", "hello")

	if ttl := store.TTL("mykey"); ttl != -1 {
		t.Fatalf("Expected TTL on persistent key to be -1, got %d", ttl)
	}

	if persisted := store.Persist("mykey"); persisted {
		t.Fatalf("Expected Persist on key without expiration to return false")
	}

	if ok := store.Expire("mykey", 2); !ok {
		t.Fatalf("Expected Expire('mykey', 2) to return true")
	}

	ttl := store.TTL("mykey")
	if ttl <= 0 || ttl > 2 {
		t.Fatalf("Expected TTL to be between 1 and 2, got %d", ttl)
	}

	val, exists := store.Get("mykey")
	if !exists || val != "hello" {
		t.Fatalf("Expected to read key before expiration, got val=%q, exists=%v", val, exists)
	}

	if persisted := store.Persist("mykey"); !persisted {
		t.Fatalf("Expected Persist to return true on expiring key")
	}
	if ttl := store.TTL("mykey"); ttl != -1 {
		t.Fatalf("Expected TTL to be -1 after PERSIST, got %d", ttl)
	}

	store.Expire("mykey", 1)
	time.Sleep(1100 * time.Millisecond)

	val, exists = store.Get("mykey")
	if exists || val != "" {
		t.Fatalf("Expected key to be lazily expired, got val=%q, exists=%v", val, exists)
	}

	if ttl := store.TTL("mykey"); ttl != -2 {
		t.Fatalf("Expected TTL to be -2 after expiration, got %d", ttl)
	}

	if store.Exists("mykey") {
		t.Fatalf("Expected Exists to be false after expiration")
	}
}

// TestStoreConcurrentAccess stress tests concurrent reads, writes, expirations, and evictions.
func TestStoreConcurrentAccess(t *testing.T) {
	// Store with capacity of 50 to heavily trigger concurrent evictions
	store := NewStoreWithConfig(50, 10*time.Millisecond)
	defer store.Close()

	numWorkers := 30
	opsPerWorker := 100

	var wg sync.WaitGroup
	wg.Add(numWorkers * 4)

	// Writers & Expirers
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("concurrent_key_%d", (workerID*opsPerWorker)+j)
				store.Set(key, "data")
				if j%3 == 0 {
					store.Expire(key, 1)
				}
			}
		}(i)
	}

	// Readers (triggers MoveToFront concurrently)
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("concurrent_key_%d", (workerID*opsPerWorker)+j)
				_, _ = store.Get(key)
				_ = store.Exists(key)
				_ = store.TTL(key)
			}
		}(i)
	}

	// Persisters
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("concurrent_key_%d", (workerID*opsPerWorker)+j)
				_ = store.Persist(key)
			}
		}(i)
	}

	// Deleters
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("concurrent_key_%d", (workerID*opsPerWorker)+j)
				_ = store.Delete(key)
			}
		}(i)
	}

	wg.Wait()
}

// verifyStoreConsistency asserts that the internal data map, elements map,
// and doubly linked list are 100% in sync with matching lengths.
func verifyStoreConsistency(t *testing.T, s *Store, expectedLen int) {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()

	dataLen := len(s.data)
	elemLen := len(s.elements)
	listLen := s.evictList.Len()

	if dataLen != expectedLen || elemLen != expectedLen || listLen != expectedLen {
		t.Fatalf("Inconsistent store state: data=%d, elements=%d, list=%d, expected=%d",
			dataLen, elemLen, listLen, expectedLen)
	}

	// Walk list from front to back to verify each key exists in data and elements
	for e := s.evictList.Front(); e != nil; e = e.Next() {
		key := e.Value.(string)
		if _, ok := s.data[key]; !ok {
			t.Fatalf("Key %q in evictList does not exist in data map", key)
		}
		if indexedElem, ok := s.elements[key]; !ok || indexedElem != e {
			t.Fatalf("Key %q in elements map does not match list element pointer", key)
		}
	}
}
