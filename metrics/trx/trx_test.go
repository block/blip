// Copyright 2026 Block, Inc.

package trx

import (
	"context"
	"errors"
	"testing"

	"github.com/cashapp/blip/v2/test/mock"
)

func TestCollectReturnsDeadlineExceeded(t *testing.T) {
	db := mock.ContextDB()
	defer db.Close()

	c := NewTrx(db)
	c.atLevel["kpi"] = trxMetrics{queryOldest: true}

	ctx, cancel := context.WithTimeout(context.Background(), -1) // CMR already reached
	defer cancel()
	_, err := c.Collect(ctx, "kpi")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Collect returned error %v, expected an error wrapping context.DeadlineExceeded", err)
	}
}
