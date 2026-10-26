// Copyright 2026 Block, Inc.

package monitor

import (
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/status"
	"github.com/cashapp/blip/v2/test/mock"
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

func TestMonitorStopEvent(t *testing.T) {
	// Monitor.Stop sends MONITOR_STOPPED for the monitor, with the same
	// message format as the Loader, and only when it stops the monitor.
	const monitorId = "stop-event"
	defer status.RemoveMonitor(monitorId)
	var mux sync.Mutex
	var stopped []event.Event
	event.Subscribe(mock.EventReceiver{RecvFunc: func(e event.Event) {
		if e.Event == event.MONITOR_STOPPED && e.MonitorId == monitorId {
			mux.Lock()
			stopped = append(stopped, e)
			mux.Unlock()
		}
	}})
	defer event.RemoveSubscribers()

	m := NewMonitor(MonitorArgs{
		Config:  blip.ConfigMonitor{MonitorId: monitorId},
		DbMaker: failingDBFactory{},
	})
	m.retry.InitialInterval = time.Minute // startup sleeps until Stop
	m.retry.Reset()
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(); err != nil { // already stopped: no event
		t.Fatal(err)
	}

	mux.Lock()
	defer mux.Unlock()
	if len(stopped) != 1 {
		t.Fatalf("got %d %s events, expected 1: %+v", len(stopped), event.MONITOR_STOPPED, stopped)
	}
	if stopped[0].MonitorId != monitorId {
		t.Errorf("event MonitorId %q, expected %q", stopped[0].MonitorId, monitorId)
	}
	if !strings.HasPrefix(stopped[0].Message, "stopped in ") {
		t.Errorf("event message %q, expected stopped in <duration>", stopped[0].Message)
	}
}
