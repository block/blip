// Copyright 2024 Block, Inc.

package server_test

import (
	"context"
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

func TestAPIMonitorsStart(t *testing.T) {
	// POST /monitors/start returns 200 when the monitor starts, and 409 when
	// it's already running or the StartMonitor plugin declines to start it.
	cfg := blip.DefaultConfig()
	ml := monitor.NewLoader(monitor.LoaderArgs{
		Config: cfg,
		Factories: blip.Factories{
			DbConn: dbconn.NewConnFactory(nil, nil),
		},
		PlanLoader: plan.NewLoader(nil),
		RDSLoader:  aws.RDSLoader{ClientFactory: mock.RDSClientFactory{}},
		Plugins: blip.Plugins{
			LoadMonitors: func(blip.Config) ([]blip.ConfigMonitor, error) {
				return []blip.ConfigMonitor{
					{MonitorId: "api-start", Hostname: "127.0.0.1:1"},
					{MonitorId: "api-declined", Hostname: "127.0.0.1:1"},
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
	ts := httptest.NewServer(server.NewAPI(cfg, ml))
	defer ts.Close()

	stopped := make(chan struct{})
	var stoppedOnce sync.Once
	event.Subscribe(mock.EventReceiver{RecvFunc: func(e event.Event) {
		if e.Event == event.MONITOR_STOPPED && e.MonitorId == "api-start" {
			stoppedOnce.Do(func() { close(stopped) })
		}
	}})
	defer event.RemoveSubscribers()

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

	// The loader sends the stopped event after the stop returns. Wait for it,
	// else it races with the next test, which sets the global event receiver.
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Error("timeout waiting for monitor-stopped event")
	}
}
