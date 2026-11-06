package storage

import (
	"sync"
	"testing"
)

func TestStore_BasicCRUD(t *testing.T) {
	s := NewStore()
	defer s.Close()

	s.Set("name", "miniredis")
	val, ok := s.Get("name")
	if !ok || val != "miniredis" {
		t.Fatalf("expected miniredis, got %q", val)
	}

	if !s.Exists("name") {
		t.Fatalf("expected key to exist")
	}

	if !s.Delete("name") {
		t.Fatalf("expected delete to return true")
	}

	if s.Exists("name") {
		t.Fatalf("expected key to be deleted")
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
