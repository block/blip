// Copyright 2026 Block, Inc.

package waitiotable

import (
	"context"
	"errors"
	"math"
	"net"
	"testing"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/test"
	_ "github.com/go-sql-driver/mysql"
)

func TestCollectSchemaLargeDecimalMySQL80(t *testing.T) {
	_, db, err := test.Connection("mysql80")
	if err != nil {
		var netErr *net.OpError
		if errors.As(err, &netErr) {
			t.Skipf("mysql80 not running: %v", err)
		}
		t.Fatalf("connect to mysql80: %v", err)
	}
	defer db.Close()

	// MySQL returns SUM of unsigned counters as DECIMAL. This value is
	// larger than MaxInt64, which the old scan destination could not hold.
	query := "SELECT 'mysql' AS OBJECT_SCHEMA, '' AS OBJECT_NAME, CAST(18446744073709551616 AS DECIMAL(30, 0)) AS sum_timer_wait"
	values, err := NewTable(db).collectQuery(context.Background(), query, nil, blip.CUMULATIVE_COUNTER, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0].Value != math.Exp2(64) || values[0].Group["db"] != "mysql" {
		t.Fatalf("unexpected schema metrics: %+v", values)
	}
}
