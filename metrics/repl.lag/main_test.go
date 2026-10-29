// Copyright 2026 Block, Inc.

package repllag

import (
	"fmt"
	"os"
	"testing"

	"github.com/cashapp/blip/v2/test"
)

// TestMain fails the run if a heartbeat reader is still running after all
// tests. Tests count only the readers that they start (test.Goroutines), so
// this catches a reader leaked by a test that doesn't count readers.
func TestMain(m *testing.M) {
	code := m.Run()
	if n := test.WaitGoroutines(test.HeartbeatReaderFunc, 0, test.HeartbeatReaderTimeout); n != 0 {
		fmt.Fprintf(os.Stderr, "FAIL: %d heartbeat readers running after all tests, expected 0: a test didn't stop the reader that it started\n", n)
		code = 1
	}
	os.Exit(code)
}
