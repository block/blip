// Copyright 2024 Block, Inc.

package status_test

import (
	"testing"

	"github.com/cashapp/blip/v2/status"
)

func TestCounters(t *testing.T) {
	status.Reset()
	defer status.Reset()

	c := status.Counter("m1", status.ENGINE_EMR_TIMEOUTS)
	c.Add(2)
	if got := status.Counter("m1", status.ENGINE_EMR_TIMEOUTS); got != c {
		t.Fatal("Counter returned a different pointer for the same monitor and name")
	}
	status.Counter("m2", status.LEVEL_TICKS_DROPPED) // created at zero

	// Setting monitor status must not reset counters
	status.Monitor("m1", status.MONITOR, "running")
	c.Add(1)

	got := status.ReportCounters()
	if got["m1"][status.ENGINE_EMR_TIMEOUTS] != 3 {
		t.Errorf("m1 %s = %d, expected 3", status.ENGINE_EMR_TIMEOUTS, got["m1"][status.ENGINE_EMR_TIMEOUTS])
	}
	if v, ok := got["m2"][status.LEVEL_TICKS_DROPPED]; !ok || v != 0 {
		t.Errorf("m2 %s = %d (reported %t), expected 0 and reported", status.LEVEL_TICKS_DROPPED, v, ok)
	}

	got = status.ReportCounters("m2")
	if _, ok := got["m1"]; ok || len(got) != 1 {
		t.Errorf("ReportCounters(m2) = %v, expected only m2", got)
	}

	status.RemoveMonitor("m1")
	if _, ok := status.ReportCounters()["m1"]; ok {
		t.Error("m1 counters reported after RemoveMonitor")
	}
}
