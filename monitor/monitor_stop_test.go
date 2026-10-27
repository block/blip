// Copyright 2026 Block, Inc.

package monitor

import (
	"database/sql"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/plan"
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

// stressDBFactory makes providers slowly and sometimes fails. It records a
// violation if two makeDB calls overlap or if a provider is made while another
// is still open, which happens only when two runLoops run at once.
type stressDBFactory struct {
	mux        sync.Mutex
	rand       *rand.Rand
	inFlight   atomic.Int32
	open       atomic.Int32
	opened     atomic.Int32
	closed     atomic.Int32
	violations atomic.Int32
}

func (f *stressDBFactory) Make(blip.ConfigMonitor) (*sql.DB, string, error) {
	return nil, "", errors.New("no plans table") // plan loading fails and retries
}

func (f *stressDBFactory) MakeProvider(blip.ConfigMonitor) (blip.DbProvider, string, error) {
	if f.inFlight.Add(1) > 1 || f.open.Load() != 0 {
		f.violations.Add(1)
	}
	defer f.inFlight.Add(-1)
	f.mux.Lock()
	d := time.Duration(f.rand.Intn(30)) * time.Millisecond
	fail := f.rand.Intn(3) == 0
	f.mux.Unlock()
	time.Sleep(d)
	if fail {
		return nil, "", errors.New("cannot make DB")
	}
	f.open.Add(1)
	f.opened.Add(1)
	return &stressDBProvider{f: f}, "stress", nil
}

type stressDBProvider struct {
	f *stressDBFactory
}

func (p *stressDBProvider) Primary() *sql.DB { return &sql.DB{} }

func (p *stressDBProvider) Close() error {
	p.f.open.Add(-1)
	p.f.closed.Add(1)
	return nil
}

func TestMonitorConcurrentStartStop(t *testing.T) {
	// Start and Stop from several goroutines at once. Stop must wait for
	// runLoop to return, so that a later Start never runs a second runLoop
	// while the first one is still making or holding database connections.
	const monitorId = "concurrent-start-stop"
	defer status.RemoveMonitor(monitorId)
	f := &stressDBFactory{rand: rand.New(rand.NewSource(1))}
	m := NewMonitor(MonitorArgs{
		Config: blip.ConfigMonitor{
			MonitorId: monitorId,
			Plans:     blip.ConfigPlans{Table: "plans"},
		},
		DbMaker:    f,
		PlanLoader: plan.NewLoader(nil),
	})
	m.retry.InitialInterval = time.Millisecond
	m.retry.MaxInterval = 5 * time.Millisecond
	m.retry.Reset()

	const goroutines, ops = 4, 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		r := rand.New(rand.NewSource(int64(g)))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				if r.Intn(2) == 0 {
					m.Start() // error if already running
				} else {
					m.Stop()
				}
				time.Sleep(time.Duration(r.Intn(2000)) * time.Microsecond)
			}
		}()
	}
	wg.Wait()
	m.Stop()

	if n := f.violations.Load(); n != 0 {
		t.Errorf("%d makeDB calls overlapped another runLoop", n)
	}
	if f.opened.Load() == 0 {
		t.Error("no database provider was made")
	}
	if opened, closed := f.opened.Load(), f.closed.Load(); opened != closed {
		t.Errorf("%d database providers made, %d closed", opened, closed)
	}
}
