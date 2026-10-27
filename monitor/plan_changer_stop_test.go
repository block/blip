// Copyright 2026 Block, Inc.

package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/cashapp/blip/v2/ha"
	"github.com/cashapp/blip/v2/status"
)

// offlineTestDriver is a fake SQL driver that can't connect, so the PCH sees
// MySQL offline.
const offlineTestDriver = "blip-monitor-offline-test"

// hungTestDriver is a fake SQL driver whose queries block until their context
// is canceled, like a hung or partitioned MySQL.
const hungTestDriver = "blip-monitor-hung-test"

func init() {
	sql.Register(offlineTestDriver, offlineTestSQLDriver{})
	sql.Register(hungTestDriver, hungTestSQLDriver{})
}

type hungTestSQLDriver struct{}

func (hungTestSQLDriver) Open(string) (driver.Conn, error) { return hungTestConn{}, nil }

type hungTestConn struct{}

func (hungTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("no prepared statements")
}
func (hungTestConn) Close() error              { return nil }
func (hungTestConn) Begin() (driver.Tx, error) { return nil, errors.New("no transactions") }
func (hungTestConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	hungTestQueries <- struct{}{}
	<-ctx.Done()
	return nil, ctx.Err()
}

var hungTestQueries = make(chan struct{}, 10) // a query has started

type offlineTestSQLDriver struct{}

func (offlineTestSQLDriver) Open(string) (driver.Conn, error) { return nil, errors.New("offline") }

// stopPCH runs a PCH on a database opened with driverName, waits until ready
// returns true, then stops the PCH and fails the test if the stop takes 1s or
// longer. While MySQL is offline, the PCH waits 1 minute between checks.
func stopPCH(t *testing.T, monitorId, driverName string, ready func() bool) {
	t.Helper()
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	pch := NewPlanChanger(PlanChangerArgs{MonitorId: monitorId, DB: db, HA: ha.Disabled})
	pch.retry.InitialInterval = time.Minute
	pch.retry.MaxInterval = time.Minute
	pch.retry.Reset()

	stopChan := make(chan struct{})
	doneChan := make(chan struct{})
	go pch.Run(stopChan, doneChan)
	deadline := time.Now().Add(5 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			close(stopChan)
			t.Fatal("timeout waiting for the PCH")
		}
		time.Sleep(5 * time.Millisecond)
	}

	t0 := time.Now()
	close(stopChan)
	select {
	case <-doneChan:
	case <-time.After(time.Minute):
		t.Fatal("PCH did not stop")
	}
	if d := time.Since(t0); d >= time.Second {
		t.Errorf("stop took %s, expected less than 1s", d)
	}
}

func TestPlanChangerStopsDuringOfflineWait(t *testing.T) {
	// While MySQL is offline, the PCH waits its retry backoff (up to about 90s:
	// 60s plus jitter) between state checks. Stopping it must not wait for that.
	const monitorId = "pch-stop"
	defer status.RemoveMonitor(monitorId)
	stopPCH(t, monitorId, offlineTestDriver, func() bool {
		return status.ReportMonitors(monitorId)[monitorId]["error:"+status.PLAN_CHANGER] != ""
	})
}

func TestPlanChangerStopsDuringStateQuery(t *testing.T) {
	// The read-only query can wait up to 10s on a hung or partitioned MySQL.
	// Stopping the PCH cancels it instead of waiting.
	const monitorId = "pch-stop-query"
	defer status.RemoveMonitor(monitorId)
	stopPCH(t, monitorId, hungTestDriver, func() bool {
		select {
		case <-hungTestQueries:
			return true
		default:
			return false
		}
	})
	// A canceled query isn't an error: the PCH didn't see MySQL offline
	if e := status.ReportMonitors(monitorId)[monitorId]["error:"+status.PLAN_CHANGER]; e != "" {
		t.Errorf("PCH error status %q after stop, expected none", e)
	}
}
