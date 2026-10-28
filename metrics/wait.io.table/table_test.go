// Copyright 2026 Block, Inc.

package waitiotable

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cashapp/blip/v2"
	blipErrors "github.com/cashapp/blip/v2/errors"
	"github.com/cashapp/blip/v2/test/mock"
)

func TestCollectReturnsDeadlineExceededReadingRows(t *testing.T) {
	tests := []struct {
		name     string
		truncate bool
	}{
		{"truncate", true},
		{"no truncate", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := mock.ContextDB("db", "tbl", "count_star")
			defer db.Close()

			o := &tableOptions{
				query:      "SELECT object_schema, object_name, count_star FROM performance_schema.table_io_waits_summary_by_table",
				truncate:   tt.truncate,
				metricType: blip.CUMULATIVE_COUNTER,
			}
			if tt.truncate {
				o.metricType = blip.DELTA_COUNTER
				o.truncateErrPolicy = blipErrors.NewTruncateErrorPolicy("")
			}
			c := NewTable(db)
			c.options["kpi"] = o

			// CMR reached while reading rows
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			metrics, err := c.Collect(ctx, "kpi")
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Collect returned error %v, expected an error wrapping context.DeadlineExceeded", err)
			}
			if metrics != nil {
				t.Errorf("Collect returned metrics %v, expected nil", metrics)
			}

			if !tt.truncate {
				return
			}
			// The table wasn't truncated, so the next sample covers two
			// intervals. The default policy (drop) must drop it rather than
			// report it as a one-interval delta.
			var stop bool
			next := []blip.MetricValue{{Name: "count_star", Type: blip.DELTA_COUNTER, Value: 20}}
			got, err := o.truncateErrPolicy.TruncateError(nil, &stop, next)
			if err != nil {
				t.Errorf("TruncateError returned error %v, expected nil", err)
			}
			if got != nil {
				t.Errorf("next sample after failed read: got metrics %v, expected nil (dropped)", got)
			}
		})
	}
}
