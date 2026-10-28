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

// countFunc is in the stack of the goroutine that calls countGoroutines.
// countGoroutines checks that it finds itself in the profile, so a change in
// the profile format fails loudly instead of counting 0.
const countFunc = "test.countGoroutines"

// maxFrames is less than the max stack depth of the goroutine profile (128
// frames by default since Go 1.23). A stack with this many frames might be
// truncated, which drops its outer frames, like heartbeat.(*BlipReader).run.
const maxFrames = 100

var lastGoroutinesId atomic.Uint64

// Goroutines is a group of goroutines: the goroutine that calls Label and the
// goroutines that it starts afterward, including goroutines that those
// goroutines start. Tests use it to detect leaked goroutines. Counts include
// only goroutines in the group, so they don't depend on test order or on how
// fast goroutines stopped by other tests exit.
type Goroutines struct {
	id string // pprof label value
}

// NewGoroutines returns a group that has no goroutines until Label is called.
func NewGoroutines() *Goroutines {
	return &Goroutines{id: strconv.FormatUint(lastGoroutinesId.Add(1), 10)}
}

// Label adds the calling goroutine to the group. Goroutines that it starts
// afterward are in the group too. A test should call Label from its own
// goroutine (the one that calls the Test or t.Run func), which exits when the
// test ends, so the label doesn't apply to other tests. Subtests started
// afterward are in the group unless they call Label on another group.
func (g *Goroutines) Label() {
	pprof.SetGoroutineLabels(pprof.WithLabels(context.Background(), pprof.Labels(goroutinesLabel, g.id)))
}

// Count returns the number of goroutines in the group with fn in their stack,
// like HeartbeatReaderFunc. It must be called from the goroutine that called
// Label.
func (g *Goroutines) Count(fn string) int {
	return countGoroutines(fn, g.id)
}

// Wait waits up to timeout for Count(fn) to equal n. It returns the last
// count, so the caller should check that it equals n.
func (g *Goroutines) Wait(fn string, n int, timeout time.Duration) int {
	return wait(func() int { return g.Count(fn) }, n, timeout)
}

// CountGoroutines returns the number of goroutines in the process with fn in
// their stack, like HeartbeatReaderFunc. The count depends on other tests, so
// use it only after all tests have run, like in TestMain.
func CountGoroutines(fn string) int {
	return countGoroutines(fn, "")
}

// WaitGoroutines waits up to timeout for CountGoroutines(fn) to equal n. It
// returns the last count, so the caller should check that it equals n.
func WaitGoroutines(fn string, n int, timeout time.Duration) int {
	return wait(func() int { return CountGoroutines(fn) }, n, timeout)
}

func wait(count func() int, n int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		got := count()
		if got == n || time.Now().After(deadline) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// countGoroutines counts goroutines with fn in their stack and, if id isn't
// empty, the group label id. It parses the goroutine profile at debug=1, which
// counts goroutines by stack and labels:
//
//	<count> @ <pc> <pc> ...
//	# labels: {"<key>":"<value>"}
//	#	<pc>	<func>+<offset>	<file>:<line>
//	...
//
// with a blank line between stacks. It panics if it doesn't find its own
// stack (with the label, if id is set) or if a counted stack might be
// truncated, because then it might count 0 for running goroutines.
func countGoroutines(fn, id string) int {
	var buf bytes.Buffer
	pprof.Lookup("goroutine").WriteTo(&buf, 1)
	_, profile, _ := strings.Cut(buf.String(), "\n") // skip "goroutine profile: total N"
	label := ""
	if id != "" {
		label = fmt.Sprintf("%q:%q", goroutinesLabel, id) // in the "# labels:" line
	}
	n := 0
	foundSelf := false
	for _, stack := range strings.Split(profile, "\n\n") {
		if label != "" && !strings.Contains(stack, label) {
			continue
		}
		if strings.Contains(stack, countFunc) {
			foundSelf = true
		}
		if !strings.Contains(stack, fn) {
			if frames := strings.Count(stack, "\n#\t"); frames >= maxFrames {
				panic(fmt.Sprintf("goroutine stack might be truncated: %d frames:\n%s", frames, stack))
			}
			continue
		}
		count, _, _ := strings.Cut(stack, " ") // "<count> @ <pc> ..."
		c, err := strconv.Atoi(count)
		if err != nil {
			panic(fmt.Sprintf("invalid goroutine profile: %s: %s", err, stack))
		}
		n += c
	}
	if !foundSelf {
		if id != "" {
			panic(fmt.Sprintf("goroutine profile has no stack with %s and label %s: Count wasn't called from the goroutine that called Label, or the profile format changed:\n%s", countFunc, label, buf.String()))
		}
		panic(fmt.Sprintf("goroutine profile has no stack with %s: the profile format changed:\n%s", countFunc, buf.String()))
	}
	return n
}
