package mongostorage

import (
	"sync"
)

type COUNTER struct {
	m   map[string]uint64
	mux sync.Mutex
}

func (c *COUNTER) Init() {
	c.mux.Lock()
	c.m = make(map[string]uint64)
	c.mux.Unlock()
} // end func Counter.Init

func (c *COUNTER) Get(name string) uint64 {
	c.mux.Lock()
	retval := c.m[name]
	c.mux.Unlock()
	return retval
} // end func Counter.Get

func (c *COUNTER) Inc(name string) {
	c.mux.Lock()
	c.m[name]++
	c.mux.Unlock()
} // end func Counter.Inc

func (c *COUNTER) Dec(name string) {
	c.mux.Lock()
	if c.m[name] > 0 {
		c.m[name]--
	}
	c.mux.Unlock()
} // end func Counter.Inc

func (c *COUNTER) Set(name string, val uint64) {
	c.mux.Lock()
	if val <= 0 {
		c.m[name] = 0
	} else {
		c.m[name] = val
	}
	c.mux.Unlock()
} // end func Counter.Set
