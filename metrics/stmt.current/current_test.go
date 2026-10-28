// Copyright 2026 Block, Inc.

package stmt

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cashapp/blip/v2/test/mock"
)

func TestCollectReturnsDeadlineExceeded(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{"query", -1},                        // CMR reached before the query
		{"read rows", 50 * time.Millisecond}, // CMR reached while reading rows
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := mock.ContextDB("timer_wait")
			defer db.Close()

			c := NewCurrent(db)
			c.atLevel["kpi"] = currentMetrics{slowest: true}

			ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()
			metrics, err := c.Collect(ctx, "kpi")
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Collect returned error %v, expected an error wrapping context.DeadlineExceeded", err)
			}
			if metrics != nil {
				t.Errorf("Collect returned metrics %v, expected nil", metrics)
			}
		})
	}
}
