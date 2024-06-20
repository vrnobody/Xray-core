package splithttp

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// reference:
// https://stackoverflow.com/questions/29923666/waiting-on-a-sync-cond-with-a-timeout
// https://gist.github.com/zviadm/c234426882bfc8acba88f3503edaaa36#file-cond2-go

// Conditional variable implementation that uses channels for notifications.
// Only supports .Broadcast() method, however supports timeout based Wait() calls
// unlike regular sync.Cond.
type CondWithTimeout struct {
	L sync.Locker
	n unsafe.Pointer
}

func NewCondWithTimeout(l sync.Locker) *CondWithTimeout {
	c := &CondWithTimeout{L: l}
	n := make(chan struct{})
	c.n = unsafe.Pointer(&n)
	return c
}

// Waits for Broadcast calls. Similar to regular sync.Cond, this unlocks the underlying
// locker first, waits on changes and re-locks it before returning.
func (c *CondWithTimeout) Wait() {
	n := c.NotifyChan()
	c.L.Unlock()
	<-n
	c.L.Lock()
}

// Same as Wait() call, but will only wait up to a given timeout.
func (c *CondWithTimeout) WaitWithTimeout(t time.Duration) bool {
	n := c.NotifyChan()
	c.L.Unlock()
	defer c.L.Lock()
	select {
	case <-n:
		return true
	case <-time.After(t):
	}
	return false
}

// Returns a channel that can be used to wait for next Broadcast() call.
func (c *CondWithTimeout) NotifyChan() <-chan struct{} {
	ptr := atomic.LoadPointer(&c.n)
	return *((*chan struct{})(ptr))
}

// Broadcast call notifies everyone that something has changed.
func (c *CondWithTimeout) Broadcast() {
	n := make(chan struct{})
	ptrOld := atomic.SwapPointer(&c.n, unsafe.Pointer(&n))
	close(*(*chan struct{})(ptrOld))
}
