// Copyright 2024 Block, Inc.

package test

import (
	"bytes"
	"context"
	"fmt"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// HeartbeatReaderFunc is in the stack of a running heartbeat.BlipReader.
	HeartbeatReaderFunc = "heartbeat.(*BlipReader).run"

	// HeartbeatReaderTimeout is how long to wait for a stopped heartbeat reader
	// to exit. It's > heartbeat.ReadTimeout: the reader exits after an in-flight
	// query.
	HeartbeatReaderTimeout = 5 * time.Second
)

const goroutinesLabel = "blip-test-goroutines"

var lastGoroutinesId atomic.Uint64

// Goroutines is a group of goroutines started by Do, including goroutines
// that those goroutines start. Tests use it to detect leaked goroutines.
// Counts include only goroutines in the group, so they don't depend on test
// order or on how fast goroutines stopped by other tests exit.
type Goroutines struct {
	id string // pprof label value
}

func NewGoroutines() *Goroutines {
	return &Goroutines{id: strconv.FormatUint(lastGoroutinesId.Add(1), 10)}
}

// Do calls f. Goroutines that f starts are in the group.
func (g *Goroutines) Do(f func()) {
	pprof.Do(context.Background(), pprof.Labels(goroutinesLabel, g.id), func(context.Context) { f() })
}

// Count returns the number of goroutines in the group with fn in their stack,
// like HeartbeatReaderFunc.
func (g *Goroutines) Count(fn string) int {
	var buf bytes.Buffer
	pprof.Lookup("goroutine").WriteTo(&buf, 1)       // 1 = count goroutines by stack and labels
	_, profile, _ := strings.Cut(buf.String(), "\n") // skip "goroutine profile: total N"
	label := fmt.Sprintf("%q:%q", goroutinesLabel, g.id)
	n := 0
	for _, stack := range strings.Split(profile, "\n\n") {
		if !strings.Contains(stack, label) || !strings.Contains(stack, fn) {
			continue
		}
		count, _, _ := strings.Cut(stack, " ") // "<count> @ <pc> ..."
		c, err := strconv.Atoi(count)
		if err != nil {
			panic(fmt.Sprintf("invalid goroutine profile: %s: %s", err, stack))
		}
		n += c
	}
	return n
}

// Wait waits up to timeout for Count(fn) to equal n. It returns the last
// count, so the caller should check that it equals n.
func (g *Goroutines) Wait(fn string, n int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		got := g.Count(fn)
		if got == n || time.Now().After(deadline) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
}
