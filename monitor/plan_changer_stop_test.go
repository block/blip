// Copyright 2026 Block, Inc.

package monitor

import (
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

func init() {
	sql.Register(offlineTestDriver, offlineTestSQLDriver{})
}

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
