package mongostorage

import (
	"sync"
)

// COUNTER is a simple struct that implements basic counters with concurrent access protection using a mutex.
type COUNTER struct {
	m   map[string]uint64
	mux sync.Mutex
}

// Init initializes the counter map by creating a new empty map.
// It should be called before using the counter to ensure that it starts with a clean state.
func (c *COUNTER) Init() {
	c.mux.Lock()
	c.m = make(map[string]uint64)
	c.mux.Unlock()
} // end func Counter.Init

// Get retrieves the current value of the counter for the given name.
func (c *COUNTER) Get(name string) uint64 {
	c.mux.Lock()
	retval := c.m[name]
	c.mux.Unlock()
	return retval
} // end func Counter.Get

// Inc increments the counter for the given name by one.
func (c *COUNTER) Inc(name string) {
	c.mux.Lock()
	c.m[name]++
	c.mux.Unlock()
} // end func Counter.Inc

// Dec decrements the counter for the given name by one, but only if the counter value is greater than 0.
func (c *COUNTER) Dec(name string) {
	c.mux.Lock()
	if c.m[name] > 0 {
		c.m[name]--
	}
	c.mux.Unlock()
} // end func Counter.Inc

// Set sets the counter for the given name to the specified val.
// If val is less than or equal to 0, the counter is set to 0.
func (c *COUNTER) Set(name string, val uint64) {
	c.mux.Lock()
	if val <= 0 {
		c.m[name] = 0
	} else {
		c.m[name] = val
	}
	c.mux.Unlock()
} // end func Counter.Set
