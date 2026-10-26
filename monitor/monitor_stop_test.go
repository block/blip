// Copyright 2026 Block, Inc.

package monitor

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/status"
)

type failingDBFactory struct{}

func (failingDBFactory) Make(blip.ConfigMonitor) (*sql.DB, string, error) {
	return nil, "", errors.New("cannot make DB")
}

func TestMonitorStopDuringStartupIsNotRunning(t *testing.T) {
	// When Stop interrupts startup (here, its retry sleep after makeDB fails),
	// startup returns nil. runLoop must not treat that as a successful start:
	// it must not clear the error or report the monitor running.
	const monitorId = "stop-during-startup"
	defer status.RemoveMonitor(monitorId)
	m := NewMonitor(MonitorArgs{
		Config:  blip.ConfigMonitor{MonitorId: monitorId},
		DbMaker: failingDBFactory{},
	})
	m.retry.InitialInterval = time.Minute // startup sleeps until Stop
	m.retry.Reset()

	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	errStatus := func() string {
		return status.ReportMonitors(monitorId)[monitorId]["error:"+status.MONITOR]
	}
	deadline := time.Now().Add(5 * time.Second)
	for errStatus() == "" {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for makeDB error")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if !m.halt() {
		t.Fatal("halt returned false for a running monitor")
	}
	if errStatus() == "" {
		t.Error("error status cleared by stop during startup")
	}
	if got := status.ReportMonitors(monitorId)[monitorId][status.MONITOR]; !strings.HasPrefix(got, "stopped at") {
		t.Errorf("monitor status %q, expected stopped at", got)
	}
}
