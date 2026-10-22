// Copyright 2026 Block, Inc.

package heartbeat

import (
	"strings"
	"testing"
)

func TestBlipReaderQuerySelectsReplCheck(t *testing.T) {
	r := NewBlipReader(BlipReaderArgs{MonitorId: "m1", Table: "blip.heartbeat", ReplCheck: "read_only"})
	if !strings.Contains(r.query, "@@read_only") {
		t.Errorf("query %q does not select @@read_only", r.query)
	}

	r = NewBlipReader(BlipReaderArgs{MonitorId: "m1", Table: "blip.heartbeat"})
	if strings.Contains(r.query, "@@") {
		t.Errorf("query %q selects a variable, expected none without repl check", r.query)
	}
}
