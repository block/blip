// Copyright 2022 Block, Inc.

package prom_test

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/prom"
	"github.com/cashapp/blip/v2/test"
	"github.com/cashapp/blip/v2/test/mock"
)

func TestAPI(t *testing.T) {
	if test.Build {
		t.Skip("doesn't work in GitHub Action")
	}

	expect := "fake output"
	exp := mock.Exporter{
		ScrapeFunc: func() (string, error) {
			return expect, nil
		},
	}

	addr := "127.0.0.1:9991"

	cfg := blip.ConfigExporter{
		Flags: map[string]string{
			"web.listen-address": "127.0.0.1:9991",
		},
	}

	api := prom.NewAPI(cfg, "mon1", exp)

	doneChan := make(chan struct{})
	go func() {
		defer close(doneChan)
		api.Run()
	}()

	url := "http://" + addr + "/metrics"

	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("got HTTP status = %d, expected %d", resp.StatusCode, http.StatusOK)
	}
	if string(body) != expect {
		t.Errorf("got '%s', expected '%s'", string(body), expect)
	}

	api.Stop()
	select {
	case <-doneChan:
	case <-time.After(300 * time.Millisecond):
		t.Error("timeout waiting for Run to return")
	}
}
