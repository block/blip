// Copyright 2026 Block, Inc.

package waitiotable

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/test/mock"
)

func TestCollectReturnsDeadlineExceededReadingRows(t *testing.T) {
	db := mock.ContextDB("db", "tbl", "count_star")
	defer db.Close()

	c := NewTable(db)
	c.options["kpi"] = &tableOptions{
		query:      "SELECT object_schema, object_name, count_star FROM performance_schema.table_io_waits_summary_by_table",
		metricType: blip.CUMULATIVE_COUNTER,
	}

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
}
