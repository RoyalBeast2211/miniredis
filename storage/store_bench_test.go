package storage

import (
	"fmt"
	"testing"
)

// Package-level global variables prevent the Go compiler from dead-code eliminating
// benchmark operations whose results are not otherwise used.
var (
	benchVal   string
	benchBool  bool
	benchInt64 int64
)

// BenchmarkStoreSet measures sequential SET throughput into an in-memory Store with no capacity limit.
func BenchmarkStoreSet(b *testing.B) {
	store := NewStoreWithInterval(0)
	defer store.Close()

	// Pre-generate keys so string formatting is not measured in the hot loop.
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.Set(keys[i], "benchmark_value")
	}
}

// BenchmarkStoreSetParallel measures concurrent SET throughput using multiple goroutines.
// This highlights mutex write-lock contention across CPU cores.
func BenchmarkStoreSetParallel(b *testing.B) {
	store := NewStoreWithInterval(0)
	defer store.Close()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("pkey_%d", i)
			store.Set(key, "benchmark_value")
			i++
		}
	})
}

// BenchmarkStoreGet measures sequential GET performance when LRU is disabled (maxKeys = 0).
// In this mode, Get uses a read lock (RLock), allowing fast concurrent access.
func BenchmarkStoreGet(b *testing.B) {
	store := NewStoreWithInterval(0)
	defer store.Close()

	// Pre-populate store with keys
	const keyCount = 10000
	for i := 0; i < keyCount; i++ {
		store.Set(fmt.Sprintf("key_%d", i), "benchmark_value")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%keyCount)
		benchVal, benchBool = store.Get(key)
	}
}

// BenchmarkStoreGetParallel measures concurrent GET performance with LRU disabled.
// Multiple goroutines acquire RLock concurrently without blocking each other.
func BenchmarkStoreGetParallel(b *testing.B) {
	store := NewStoreWithInterval(0)
	defer store.Close()

	const keyCount = 10000
	for i := 0; i < keyCount; i++ {
		store.Set(fmt.Sprintf("key_%d", i), "benchmark_value")
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key_%d", i%keyCount)
			benchVal, benchBool = store.Get(key)
			i++
		}
	})
}

// BenchmarkStoreGetLRU measures sequential GET performance when LRU is enabled (maxKeys > 0).
// Because LRU mutates the linked list on every access (MoveToFront), Get must acquire an exclusive Lock.
func BenchmarkStoreGetLRU(b *testing.B) {
	const capacity = 10000
	store := NewStoreWithConfig(capacity, 0)
	defer store.Close()

	for i := 0; i < capacity; i++ {
		store.Set(fmt.Sprintf("key_%d", i), "benchmark_value")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%capacity)
		benchVal, benchBool = store.Get(key)
	}
}

// BenchmarkStoreGetLRUParallel measures concurrent GET performance when LRU is enabled.
// Unlike BenchmarkStoreGetParallel, all reader goroutines now contend for the exclusive write lock
// because every Get must update the LRU list order.
func BenchmarkStoreGetLRUParallel(b *testing.B) {
	const capacity = 10000
	store := NewStoreWithConfig(capacity, 0)
	defer store.Close()

	for i := 0; i < capacity; i++ {
		store.Set(fmt.Sprintf("key_%d", i), "benchmark_value")
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key_%d", i%capacity)
			benchVal, benchBool = store.Get(key)
			i++
		}
	})
}

// BenchmarkStoreDel measures sequential DEL performance on existing keys.
func BenchmarkStoreDel(b *testing.B) {
	store := NewStoreWithInterval(0)
	defer store.Close()

	// Pre-populate
	for i := 0; i < b.N; i++ {
		store.Set(fmt.Sprintf("key_%d", i), "benchmark_value")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchBool = store.Delete(fmt.Sprintf("key_%d", i))
	}
}

// BenchmarkStoreExists measures sequential EXISTS checks on an in-memory Store.
func BenchmarkStoreExists(b *testing.B) {
	store := NewStoreWithInterval(0)
	defer store.Close()

	const keyCount = 10000
	for i := 0; i < keyCount; i++ {
		store.Set(fmt.Sprintf("key_%d", i), "benchmark_value")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%keyCount)
		benchBool = store.Exists(key)
	}
}

// BenchmarkStoreTTLLookup measures the cost of looking up TTL deadlines on keys.
func BenchmarkStoreTTLLookup(b *testing.B) {
	store := NewStoreWithInterval(0)
	defer store.Close()

	const keyCount = 10000
	for i := 0; i < keyCount; i++ {
		store.Set(fmt.Sprintf("key_%d", i), "benchmark_value")
		store.Expire(fmt.Sprintf("key_%d", i), 3600)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%keyCount)
		benchInt64 = store.TTL(key)
	}
}

// BenchmarkStoreLRUEviction measures SET throughput when the store is full,
// forcing an eviction of the least recently used key on every insertion.
func BenchmarkStoreLRUEviction(b *testing.B) {
	const capacity = 1000
	store := NewStoreWithConfig(capacity, 0)
	defer store.Close()

	// Fill store to capacity
	for i := 0; i < capacity; i++ {
		store.Set(fmt.Sprintf("init_key_%d", i), "benchmark_value")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("evict_key_%d", i)
		store.Set(key, "benchmark_value")
	}
}
