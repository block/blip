// Copyright 2026 Block, Inc.

package stopchan_test

import (
	"testing"
	"time"

	"github.com/cashapp/blip/v2/internal/stopchan"
)

func TestSleep(t *testing.T) {
	stopChan := make(chan struct{})
	if !stopchan.Sleep(stopChan, time.Millisecond) {
		t.Error("Sleep returned false before stop")
	}
	close(stopChan)
	t0 := time.Now()
	if stopchan.Sleep(stopChan, time.Minute) {
		t.Error("Sleep returned true after stop")
	}
	if d := time.Since(t0); d > time.Second {
		t.Errorf("Sleep after stop took %s", d)
	}
}

func TestContext(t *testing.T) {
	stopChan := make(chan struct{})
	ctx, cancel := stopchan.Context(stopChan)
	defer cancel()
	if ctx.Err() != nil {
		t.Fatalf("context done before stop: %s", ctx.Err())
	}
	close(stopChan)
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("context not canceled on stop")
	}

	// cancel without stop
	ctx, cancel = stopchan.Context(make(chan struct{}))
	cancel()
	if ctx.Err() == nil {
		t.Error("context not canceled by cancel")
	}
}
