// Copyright 2024 Block, Inc.

package server_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/aws"
	"github.com/cashapp/blip/v2/dbconn"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/monitor"
	"github.com/cashapp/blip/v2/plan"
	"github.com/cashapp/blip/v2/server"
	"github.com/cashapp/blip/v2/status"
	"github.com/cashapp/blip/v2/test"
	"github.com/cashapp/blip/v2/test/mock"
)

type testAPI struct {
	cfg blip.Config
	api *server.API
	ts  *httptest.Server
	url string
}

func setup(t *testing.T) testAPI {
	cfg := blip.DefaultConfig()
	ml := monitor.NewLoader(monitor.LoaderArgs{
		Config: cfg,
		Factories: blip.Factories{
			DbConn: dbconn.NewConnFactory(nil, nil),
		},
		PlanLoader: plan.NewLoader(nil),
		RDSLoader:  aws.RDSLoader{ClientFactory: mock.RDSClientFactory{}},
	})
	api := server.NewAPI(cfg, ml)

	ts := httptest.NewServer(api)

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	return testAPI{
		cfg: cfg,
		api: api,
		ts:  ts,
		url: fmt.Sprintf("http://%s", u.Host),
	}
}

func TestAPIStatusGet(t *testing.T) {
	server := setup(t)
	defer server.ts.Close()

	var gotStatus map[string]string
	url := server.url + "/status"
	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &gotStatus)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusOK {
		t.Errorf("got HTTP status = %d, expected %d", statusCode, http.StatusOK)
	}
	if _, ok := gotStatus["uptime"]; !ok {
		t.Errorf("/status response does not have uptime: %+v", gotStatus)
	}
}

func TestAPIStatusMonitorsCountersGet(t *testing.T) {
	server := setup(t)
	defer server.ts.Close()

	const monitorId = "api-counters"
	defer status.RemoveMonitor(monitorId)
	status.Counter(monitorId, status.ENGINE_EMR_TIMEOUTS).Add(3)

	var got map[string]map[string]uint64
	url := server.url + "/status/monitors/counters"
	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &got)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusOK {
		t.Errorf("got HTTP status = %d, expected %d", statusCode, http.StatusOK)
	}
	if got[monitorId][status.ENGINE_EMR_TIMEOUTS] != 3 {
		t.Errorf("/status/monitors/counters = %v, expected %s=3 for %s", got, status.ENGINE_EMR_TIMEOUTS, monitorId)
	}
}

// blockingDbFactory blocks Make for one monitor until release is closed, so a
// stop of that monitor during startup is stuck until then.
type blockingDbFactory struct {
	blip.DbFactory
	monitorId string
	entered   chan struct{} // closed when Make is first called for monitorId
	once      *sync.Once
	release   chan struct{}
}

func (f blockingDbFactory) Make(cfg blip.ConfigMonitor) (*sql.DB, string, error) {
	if cfg.MonitorId == f.monitorId {
		f.once.Do(func() { close(f.entered) })
		<-f.release
	}
	return f.DbFactory.Make(cfg)
}

func TestAPIMonitorsStart(t *testing.T) {
	// POST /monitors/start returns 200 when the monitor starts, 202 when a
	// monitor with the same ID is still stopping, and 409 when it's already
	// running or the StartMonitor plugin declines to start it. A monitor
	// stopped by POST /monitors/stop stays stopped across a reload.
	cfg := blip.DefaultConfig()
	dbf := blockingDbFactory{
		DbFactory: dbconn.NewConnFactory(nil, nil),
		monitorId: "api-pending",
		entered:   make(chan struct{}),
		once:      &sync.Once{},
		release:   make(chan struct{}),
	}
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(dbf.release) })
	ml := monitor.NewLoader(monitor.LoaderArgs{
		Config:     cfg,
		Factories:  blip.Factories{DbConn: dbf},
		PlanLoader: plan.NewLoader(nil),
		RDSLoader:  aws.RDSLoader{ClientFactory: mock.RDSClientFactory{}},
		Plugins: blip.Plugins{
			LoadMonitors: func(blip.Config) ([]blip.ConfigMonitor, error) {
				return []blip.ConfigMonitor{
					{MonitorId: "api-start", Hostname: "127.0.0.1:1"},
					{MonitorId: "api-declined", Hostname: "127.0.0.1:1"},
					{MonitorId: "api-pending", Hostname: "127.0.0.1:1"},
					{MonitorId: "api-new\nline", Hostname: "127.0.0.1:1"},
				}, nil
			},
			StartMonitor: func(cfg blip.ConfigMonitor) bool { return cfg.MonitorId != "api-declined" },
		},
	})
	if err := ml.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer status.RemoveMonitor("api-start")
	defer status.RemoveMonitor("api-declined")
	defer status.RemoveMonitor("api-pending")
	defer status.RemoveMonitor("api-new\nline")
	ts := httptest.NewServer(server.NewAPI(cfg, ml))
	defer ts.Close()

	var mux sync.Mutex
	stopped := map[string]int{} // monitor ID => monitor-stopped events
	event.Subscribe(mock.EventReceiver{RecvFunc: func(e event.Event) {
		if e.Event == event.MONITOR_STOPPED {
			mux.Lock()
			stopped[e.MonitorId]++
			mux.Unlock()
		}
	}})
	defer event.RemoveSubscribers()

	// A stop that returns 200 has already sent the stopped event, because
	// Loader.Stop waits until the stop has finished, so for those waitStopped
	// is only a check. A stop that returns 202 sends it later, in the
	// background: wait for it, else it races with the next test, which sets
	// the global event receiver.
	waitStopped := func(monitorId string, n int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			mux.Lock()
			got := stopped[monitorId]
			mux.Unlock()
			if got >= n {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("timeout waiting for %d monitor-stopped events for %s, got %d", n, monitorId, got)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	post := func(path string) int {
		t.Helper()
		resp, err := http.Post(ts.URL+path, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if got := post("/monitors/start?id=api-start"); got != http.StatusOK {
		t.Errorf("first start: got %d, expected 200", got)
	}
	if got := post("/monitors/start?id=api-start"); got != http.StatusConflict {
		t.Errorf("start running monitor: got %d, expected 409", got)
	}
	if got := post("/monitors/start?id=api-declined"); got != http.StatusConflict {
		t.Errorf("start declined monitor: got %d, expected 409", got)
	}
	if got := post("/monitors/stop?id=api-start"); got != http.StatusOK {
		t.Errorf("stop: got %d, expected 200", got)
	}
	waitStopped("api-start", 1)

	// An ID with CR or LF: the API removes them for logging, but it must
	// start and stop the monitor with the real ID. If the stop didn't stop
	// it, the second start would return 409 (already running).
	const nlId = "api-new\nline"
	nlPath := "?id=" + url.QueryEscape(nlId)
	for i := 1; i <= 2; i++ {
		if got := post("/monitors/start" + nlPath); got != http.StatusOK {
			t.Errorf("start %d of ID with newline: got %d, expected 200", i, got)
		}
		if got := post("/monitors/stop" + nlPath); got != http.StatusOK {
			t.Errorf("stop %d of ID with newline: got %d, expected 200", i, got)
		}
		waitStopped(nlId, i)
	}

	// Start api-pending and stop it while it's stuck in startup (making its
	// DB), so the stop times out after the default stop timeout (3s). Starting
	// it while it's still stopping is pending, and starting it again is still
	// pending, not already running.
	if got := post("/monitors/start?id=api-pending"); got != http.StatusOK {
		t.Errorf("start api-pending: got %d, expected 200", got)
	}
	select {
	case <-dbf.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for api-pending to make its DB")
	}
	if got := post("/monitors/stop?id=api-pending"); got != http.StatusAccepted {
		t.Errorf("stop stuck monitor: got %d, expected 202", got)
	}
	for i := 1; i <= 2; i++ {
		if got := post("/monitors/start?id=api-pending"); got != http.StatusAccepted {
			t.Errorf("start %d while stopping: got %d, expected 202", i, got)
		}
	}

	// When the old run stops, the pending start runs, and then the monitor
	// is already running
	releaseOnce.Do(func() { close(dbf.release) })
	waitStopped("api-pending", 1)
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := post("/monitors/start?id=api-pending")
		if got == http.StatusConflict {
			break
		}
		if got != http.StatusAccepted || time.Now().After(deadline) {
			t.Fatalf("start after stuck stop finished: got %d, expected 409 (already running)", got)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := post("/monitors/stop?id=api-pending"); got != http.StatusOK {
		t.Errorf("stop api-pending: got %d, expected 200", got)
	}
	waitStopped("api-pending", 2)

	// A monitor stopped through the API stays stopped across a reload, which
	// doesn't change it, until it's started: the start returns 200, not 409
	// (already running)
	if got := post("/monitors/reload"); got != http.StatusOK {
		t.Errorf("reload: got %d, expected 200", got)
	}
	if got := post("/monitors/start?id=api-start"); got != http.StatusOK {
		t.Errorf("start after stop and reload: got %d, expected 200", got)
	}
	if got := post("/monitors/stop?id=api-start"); got != http.StatusOK {
		t.Errorf("stop api-start: got %d, expected 200", got)
	}
	waitStopped("api-start", 2)
}
