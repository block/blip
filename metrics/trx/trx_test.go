// Copyright 2024 Block, Inc.

package trx

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cashapp/blip/v2/test/mock"
)

func TestCollectWrapsQueryError(t *testing.T) {
	db := mock.ErrorDB(fmt.Errorf("query: %w", context.DeadlineExceeded))
	defer db.Close()

	c := NewTrx(db)
	c.atLevel["kpi"] = trxMetrics{queryOldest: true}

	_, err := c.Collect(context.Background(), "kpi")
	if err == nil {
		t.Fatal("Collect returned nil error, expected query error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("errors.Is(err, context.DeadlineExceeded) = false, expected true; err: %v", err)
	}
}
