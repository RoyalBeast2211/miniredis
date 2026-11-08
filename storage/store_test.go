package storage

import (
	"sync"
	"testing"
	"time"
)

func TestStore_TTL(t *testing.T) {
	s := NewStore()
	defer s.Close()

	s.Set("k", "v")
	if ttl := s.TTL("k"); ttl != -1 {
		t.Fatalf("expected -1 for permanent key, got %d", ttl)
	}

	s.Expire("k", 10)
	if ttl := s.TTL("k"); ttl <= 0 || ttl > 10 {
		t.Fatalf("expected TTL ~10, got %d", ttl)
	}

	if !s.Persist("k") {
		t.Fatalf("expected persist to succeed")
	}
	if ttl := s.TTL("k"); ttl != -1 {
		t.Fatalf("expected -1 after persist, got %d", ttl)
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	s := NewStore()
	defer s.Close()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			s.Set("key", "val")
			s.Get("key")
			s.Exists("key")
		}(i)
	}
	wg.Wait()
}
