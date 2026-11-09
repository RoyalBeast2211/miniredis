package storage

import (
	"container/list"
	"math"
	"sync"
	"time"
)

type Store struct {
	mu          sync.RWMutex
	data        map[string]string
	expires     map[string]time.Time
	maxKeys     int
	evictList   *list.List
	elements    map[string]*list.Element
	stopCleanup chan struct{}
}

func NewStore() *Store {
	return NewStoreWithCapacity(0)
}

func NewStoreWithCapacity(maxKeys int) *Store {
	s := &Store{
		data:        make(map[string]string),
		expires:     make(map[string]time.Time),
		maxKeys:     maxKeys,
		evictList:   list.New(),
		elements:    make(map[string]*list.Element),
		stopCleanup: make(chan struct{}),
	}
	go s.startActiveCleanup(1 * time.Second)
	return s
}

func (s *Store) Close() {
	select {
	case <-s.stopCleanup:
	default:
		close(s.stopCleanup)
	}
}

func (s *Store) startActiveCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCleanup:
			return
		case <-ticker.C:
			s.cleanExpiredKeys()
		}
	}
}

func (s *Store) cleanExpiredKeys() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for key, deadline := range s.expires {
		if now.After(deadline) {
			delete(s.data, key)
			delete(s.expires, key)
		}
	}
}

func (s *Store) isExpiredLocked(key string) bool {
	deadline, hasExpiry := s.expires[key]
	if !hasExpiry {
		return false
	}
	return time.Now().After(deadline)
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	delete(s.expires, key)
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	val, exists := s.data[key]
	if !exists {
		s.mu.RUnlock()
		return "", false
	}
	if s.isExpiredLocked(key) {
		s.mu.RUnlock()
		s.Delete(key)
		return "", false
	}
	s.mu.RUnlock()
	return val, true
}

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.data[key]; exists {
		delete(s.data, key)
		delete(s.expires, key)
		return true
	}
	return false
}

func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	_, exists := s.data[key]
	if !exists {
		s.mu.RUnlock()
		return false
	}
	if s.isExpiredLocked(key) {
		s.mu.RUnlock()
		s.Delete(key)
		return false
	}
	s.mu.RUnlock()
	return true
}

func (s *Store) Expire(key string, seconds int64) bool {
	return s.ExpireAt(key, time.Now().Unix()+seconds)
}

func (s *Store) ExpireAt(key string, unixSeconds int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.data[key]; !exists {
		return false
	}
	deadline := time.Unix(unixSeconds, 0)
	if time.Now().After(deadline) {
		delete(s.data, key)
		delete(s.expires, key)
		return true
	}
	s.expires[key] = deadline
	return true
}

func (s *Store) TTL(key string) int64 {
	s.mu.RLock()
	_, exists := s.data[key]
	if !exists {
		s.mu.RUnlock()
		return -2
	}
	deadline, hasExpiry := s.expires[key]
	s.mu.RUnlock()
	if !hasExpiry {
		return -1
	}
	now := time.Now()
	if now.After(deadline) {
		s.Delete(key)
		return -2
	}
	return int64(math.Ceil(deadline.Sub(now).Seconds()))
}

func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.data[key]; !exists {
		return false
	}
	if _, hasExpiry := s.expires[key]; !hasExpiry {
		return false
	}
	delete(s.expires, key)
	return true
}
