// Copyright 2024 Block, Inc.

// Package prom provides Prometheus emulation and translation.
package prom

import (
	"context"
	"io"
	"net/http"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/status"
)

// Exporter emulates a Prometheus mysqld_exporter.
type Exporter interface {
	Scrape() (string, error)
}

// API emulates a Prometheus exporter API. It uses an Exporter to scape metrics
// when GET /metrics is called.
type API struct {
	cfg       blip.ConfigExporter
	monitorId string
	exp       Exporter
	// --
	srv *http.Server
}

func NewAPI(cfg blip.ConfigExporter, monitorId string, exp Exporter) *API {
	return &API{
		cfg:       cfg,
		monitorId: monitorId,
		exp:       exp,
		// --
		srv: &http.Server{
			Addr: blip.SetOrDefault(cfg.Flags["web.listen-address"], blip.DEFAULT_EXPORTER_LISTEN_ADDR),
		},
	}
}

func (api *API) Run() error {
	blip.Debug("%s: prom addr %s", api.monitorId, api.srv.Addr)
	status.Monitor(api.monitorId, "exporter", "listening on %s", api.srv.Addr)
	defer status.Monitor(api.monitorId, "exporter", "stopped")

	path := blip.SetOrDefault(api.cfg.Flags["web.telemetry-path"], blip.DEFAULT_EXPORTER_PATH)
	mux := http.NewServeMux()
	mux.HandleFunc(path, api.metricsHandler)
	api.srv.Handler = mux

	err := api.srv.ListenAndServe() // blocks
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (api *API) Stop() {
	api.srv.Shutdown(context.Background())
}

func (api *API) metricsHandler(w http.ResponseWriter, r *http.Request) {
	expo, err := api.exp.Scrape()
	if err != nil {
		blip.Debug(err.Error())
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, expo)
}
