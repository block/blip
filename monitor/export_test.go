// Copyright 2026 Block, Inc.

package monitor

// StopAfterTicks makes every LCO Run return at tick n+1, before it collects at
// that tick, by closing stopChan. Use it for a test that runs one LCO, and call
// the returned func to remove it.
func StopAfterTicks(n uint, stopChan chan struct{}) func() {
	tickerMux.Lock()
	tickHook = func(tick uint) {
		if tick == n+1 {
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
