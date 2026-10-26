// Copyright 2026 Block, Inc.

// Package stopchan has helpers for goroutines that stop when a stop channel is
// closed, like the monitor subsystems. It's internal so that both the monitor
// and heartbeat packages can use it without adding to Blip's public API.
package stopchan

import (
	"context"
	"time"
)

// Sleep waits for d or until stopChan is closed. It returns false if stopChan
// was closed.
func Sleep(stopChan <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-stopChan:
		return false
	case <-t.C:
		return true
	}
}

// Context returns a context that's canceled when stopChan is closed, so a
// query or other call that takes the context returns on stop. Call cancel when
// done with the context to release its resources.
func Context(stopChan <-chan struct{}) (ctx context.Context, cancel context.CancelFunc) {
	ctx, cancel = context.WithCancel(context.Background())
	go func() {
		select {
		case <-stopChan:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
