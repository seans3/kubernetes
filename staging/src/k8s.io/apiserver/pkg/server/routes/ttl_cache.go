package routes

import (
	"sync"
	"time"
)

// TTLCache provides a thread-safe, lazy-loaded cache with a time-to-live.
// It builds the value on the first Get() and evicts it after the TTL has
// elapsed since the last Get() call.
type TTLCache[T any] struct {
	mu        sync.Mutex
	ttl       time.Duration
	buildFunc func() (T, string, error)

	cachedValue T
	cachedEtag  string
	cachedErr   error
	isCached    bool
	timer       *time.Timer

	// Dependencies for testing
	afterFunc func(d time.Duration, f func()) *time.Timer
}

// NewTTLCache creates a new TTLCache.
func NewTTLCache[T any](ttl time.Duration, buildFunc func() (T, string, error)) *TTLCache[T] {
	return &TTLCache[T]{
		ttl:       ttl,
		buildFunc: buildFunc,
		afterFunc: time.AfterFunc,
	}
}

// Get returns the cached value, building it if necessary, and resets the TTL timer.
func (c *TTLCache[T]) Get() (T, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isCached {
		if c.timer != nil {
			c.timer.Reset(c.ttl)
		}
		return c.cachedValue, c.cachedEtag, c.cachedErr
	}

	c.cachedValue, c.cachedEtag, c.cachedErr = c.buildFunc()
	c.isCached = true

	c.timer = c.afterFunc(c.ttl, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		var zero T
		c.cachedValue = zero
		c.cachedEtag = ""
		c.cachedErr = nil
		c.isCached = false
	})

	return c.cachedValue, c.cachedEtag, c.cachedErr
}
