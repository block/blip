// Copyright 2024 Block, Inc.

package test

import (
	"bytes"
	"runtime/pprof"
	"strings"
	"time"
)

// Goroutines returns the number of goroutines with fn in their stack, like
// "heartbeat.(*BlipReader).run". Tests use it to detect leaked goroutines.
func Goroutines(fn string) int {
	var buf bytes.Buffer
	pprof.Lookup("goroutine").WriteTo(&buf, 2) // 2 = one stack per goroutine
	n := 0
	for _, stack := range strings.Split(buf.String(), "\n\n") {
		if strings.Contains(stack, fn) {
			n++
		}
	}
	return n
}

// WaitGoroutines waits up to timeout for Goroutines(fn) to equal n. It returns
// the last count, so the caller should check that it equals n.
func WaitGoroutines(fn string, n int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		got := Goroutines(fn)
		if got == n || time.Now().After(deadline) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
}
