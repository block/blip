// Copyright 2026 Block, Inc.

package monitor

import "sync/atomic"

// StopAfterTicks makes every LCO Run return at tick n+1, before it collects at
// that tick, by closing stopChan. Use it for a test that runs one LCO, and call
// the returned func to remove it. Ticks are counted across all LCOs, and
// stopChan is closed only once.
func StopAfterTicks(n uint, stopChan chan struct{}) func() {
	var ticks atomic.Uint64
	tickerMux.Lock()
	tickHook = func() {
		if ticks.Add(1) == uint64(n)+1 {
			close(stopChan)
		}
	}
	tickerMux.Unlock()
	return func() {
		tickerMux.Lock()
		tickHook = nil
		tickerMux.Unlock()
	}
}
