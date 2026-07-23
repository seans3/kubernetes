package routes

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTTLCache_BuildsOnce(t *testing.T) {
	var buildCount int32
	cache := NewTTLCache(10*time.Minute, func() (string, string, error) {
		atomic.AddInt32(&buildCount, 1)
		return "hello", "etag1", nil
	})

	// Disable actual timer logic for this test to avoid background goroutines
	cache.afterFunc = func(d time.Duration, f func()) *time.Timer {
		return nil
	}

	val1, etag1, err1 := cache.Get()
	if val1 != "hello" || etag1 != "etag1" || err1 != nil {
		t.Errorf("Expected 'hello', 'etag1', nil, got %v, %v, %v", val1, etag1, err1)
	}

	val2, etag2, err2 := cache.Get()
	if val2 != "hello" || etag2 != "etag1" || err2 != nil {
		t.Errorf("Expected 'hello', 'etag1', nil, got %v, %v, %v", val2, etag2, err2)
	}

	if atomic.LoadInt32(&buildCount) != 1 {
		t.Errorf("Expected build to be called once, got %d", buildCount)
	}
}

func TestTTLCache_EvictsAndRebuilds(t *testing.T) {
	var buildCount int32
	var timerFunc func()
	var mu sync.Mutex

	cache := NewTTLCache(1*time.Minute, func() (string, string, error) {
		atomic.AddInt32(&buildCount, 1)
		return "data", "etag2", nil
	})

	cache.afterFunc = func(d time.Duration, f func()) *time.Timer {
		mu.Lock()
		defer mu.Unlock()
		timerFunc = f
		return nil
	}

	// 1st Get
	cache.Get()
	if atomic.LoadInt32(&buildCount) != 1 {
		t.Fatalf("Expected 1 build, got %d", buildCount)
	}

	// Fetch the timer function that was scheduled
	mu.Lock()
	f := timerFunc
	mu.Unlock()

	// Simulate TTL expiration
	f()

	// 2nd Get (should rebuild since cache was evicted)
	cache.Get()
	if atomic.LoadInt32(&buildCount) != 2 {
		t.Fatalf("Expected 2 builds after eviction, got %d", buildCount)
	}
}
