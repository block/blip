// Copyright 2024 Block, Inc.

package server_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/aws"
	"github.com/cashapp/blip/v2/dbconn"
	"github.com/cashapp/blip/v2/monitor"
	"github.com/cashapp/blip/v2/plan"
	"github.com/cashapp/blip/v2/server"
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
