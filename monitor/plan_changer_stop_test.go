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

func TestPlanChangerStopsDuringOfflineWait(t *testing.T) {
	// While MySQL is offline, the PCH waits its retry backoff (up to 60s)
	// between state checks. Stopping it must not wait for that.
	const monitorId = "pch-stop"
	defer status.RemoveMonitor(monitorId)
	db, err := sql.Open(offlineTestDriver, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const wait = time.Minute
	pch := NewPlanChanger(PlanChangerArgs{MonitorId: monitorId, DB: db, HA: ha.Disabled})
	pch.retry.InitialInterval = wait
	pch.retry.MaxInterval = wait
	pch.retry.Reset()

	stopChan := make(chan struct{})
	doneChan := make(chan struct{})
	go pch.Run(stopChan, doneChan)

	// Wait for the first state check (offline), then stop during the wait
	deadline := time.Now().Add(5 * time.Second)
	for status.ReportMonitors(monitorId)[monitorId]["error:"+status.PLAN_CHANGER] == "" {
		if time.Now().After(deadline) {
			close(stopChan)
			t.Fatal("timeout waiting for PCH state check")
		}
		time.Sleep(5 * time.Millisecond)
	}
	t0 := time.Now()
	close(stopChan)
	select {
	case <-doneChan:
	case <-time.After(2 * wait):
		t.Fatal("PCH did not stop")
	}
	if d := time.Since(t0); d > wait/10 {
		t.Errorf("stop during offline wait took %s, expected well under %s", d, wait)
	}
}

func TestPlanChangerStopsDuringStateQuery(t *testing.T) {
	// The read-only query can wait up to readOnlyTimeout on a hung or
	// partitioned MySQL. Stopping the PCH cancels it instead of waiting.
	const monitorId = "pch-stop-query"
	defer status.RemoveMonitor(monitorId)
	db, err := sql.Open(hungTestDriver, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const timeout = time.Minute
	defer func(d time.Duration) { readOnlyTimeout = d }(readOnlyTimeout)
	readOnlyTimeout = timeout

	pch := NewPlanChanger(PlanChangerArgs{MonitorId: monitorId, DB: db, HA: ha.Disabled})
	stopChan := make(chan struct{})
	doneChan := make(chan struct{})
	go pch.Run(stopChan, doneChan)
	select {
	case <-hungTestQueries:
	case <-time.After(5 * time.Second):
		close(stopChan)
		t.Fatal("timeout waiting for read-only query")
	}

	t0 := time.Now()
	close(stopChan)
	select {
	case <-doneChan:
	case <-time.After(2 * timeout):
		t.Fatal("PCH did not stop")
	}
	if d := time.Since(t0); d > timeout/10 {
		t.Errorf("stop during read-only query took %s, expected well under %s", d, timeout)
	}
	// A canceled query isn't an error: the PCH didn't see MySQL offline
	if e := status.ReportMonitors(monitorId)[monitorId]["error:"+status.PLAN_CHANGER]; e != "" {
		t.Errorf("PCH error status %q after stop, expected none", e)
	}
}
