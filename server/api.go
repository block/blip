// Copyright 2024 Block, Inc.

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"runtime"
	"strings"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/event"
	"github.com/cashapp/blip/v2/metrics"
	"github.com/cashapp/blip/v2/monitor"
	"github.com/cashapp/blip/v2/sink"
	"github.com/cashapp/blip/v2/status"
)

type API struct {
	cfg           blip.Config
	monitorLoader *monitor.Loader
	// --
	httpServer *http.Server
	startTs    time.Time
}

func NewAPI(cfg blip.Config, ml *monitor.Loader) *API {
	api := &API{
		cfg:           cfg,
		monitorLoader: ml,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/config", api.config)
	mux.HandleFunc("/registered", api.registered)
	mux.HandleFunc("/version", api.version)

	mux.HandleFunc("/monitors", api.monitors)
	mux.HandleFunc("/monitors/stop", api.monitorsStop)
	mux.HandleFunc("/monitors/start", api.monitorsStart)
	mux.HandleFunc("/monitors/reload", api.monitorsReload)

	mux.HandleFunc("/status", api.status)
	mux.HandleFunc("/status/monitors", api.statusMonitors)

	mux.HandleFunc("/debug", api.debug)

	api.httpServer = &http.Server{
		Addr:    cfg.API.Bind,
		Handler: mux,
	}

	return api
}

// ServeHTTP allows the API to statisfy the http.HandlerFunc interface.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	api.httpServer.Handler.ServeHTTP(w, r)
}

// Run runs the API and restarts on error.
func (api *API) Run() error {
	api.startTs = time.Now()
	errChan := make(chan error, 1)
	for {
		go func() {
			var serr error // http Sever error
			defer func() { // catch panic
				if r := recover(); r != nil {
					b := make([]byte, 4096)
					n := runtime.Stack(b, false)
					serr = fmt.Errorf("PANIC: server API: %s\n%s", r, string(b[0:n]))
				}
				errChan <- serr
			}()
			blip.Debug("listen")
			serr = api.httpServer.ListenAndServe()
		}()
		err := <-errChan
		switch err {
		case http.ErrServerClosed:
			blip.Debug("shutdown")
			return nil
		default:
			event.Errorf(event.SERVER_API_ERROR, "%s", err.Error())
			time.Sleep(1 * time.Second) // between crashes
		}
	}
}

// --------------------------------------------------------------------------
// Blip endpoints
// --------------------------------------------------------------------------

func (api *API) config(w http.ResponseWriter, r *http.Request) {
	blip.Debug("%v", r)
	q := r.URL.Query()
	if q.Has("json") {
		json.NewEncoder(w).Encode(api.cfg)
	} else {
		yaml.NewEncoder(w).Encode(api.cfg)
	}
}

type registeredList struct {
	Collectors []string `json:"collectors"`
	Sinks      []string `json:"sinks"`
}

func (api *API) registered(w http.ResponseWriter, r *http.Request) {
	blip.Debug("%v", r)
	rl := registeredList{
		Collectors: metrics.List(),
		Sinks:      sink.List(),
	}
	json.NewEncoder(w).Encode(rl)
}

func (api *API) version(w http.ResponseWriter, r *http.Request) {
	blip.Debug("%v", r)
	json.NewEncoder(w).Encode(blip.VERSION)
}

func (api *API) debug(w http.ResponseWriter, r *http.Request) {
	blip.Debug("%v", r)
	blip.Debugging = !blip.Debugging
	json.NewEncoder(w).Encode(map[string]bool{"debugging": blip.Debugging})
}

// --------------------------------------------------------------------------
// Monitor endpoints
// --------------------------------------------------------------------------

func (api *API) monitors(w http.ResponseWriter, r *http.Request) {
	blip.Debug("%v", r)
	ml := map[string]string{}
	m := api.monitorLoader.Monitors()
	for i := range m {
		ml[m[i].MonitorId()] = m[i].DSN() // redacted
	}
	json.NewEncoder(w).Encode(ml)
}

func (api *API) monitorsReload(w http.ResponseWriter, r *http.Request) {
	blip.Debug("%v", r)
	if err := api.monitorLoader.Load(context.Background()); err != nil {
		switch err {
		case monitor.ErrStopLoss:
			http.Error(w, "Stop-loss prevented reloading; see Blip error log for details", http.StatusPreconditionFailed)
		default:
			errMsg := html.EscapeString(fmt.Sprintf("Error reloading monitors: %s", err))
			http.Error(w, errMsg, http.StatusConflict)
		}
		return
	}

	api.monitorLoader.StartMonitors()
	w.WriteHeader(http.StatusOK)
}

func (api *API) monitorsStart(w http.ResponseWriter, r *http.Request) {
	blip.Debug("%v", r)
	monitorId, mon, ok := api.monitorId(w, r)
	if !ok {
		return // monitorId() wrote error response
	}
	blip.Debug("start %s", monitorId)
	if err := mon.Start(); err != nil {
		errMsg := html.EscapeString(fmt.Sprintf("Error starting monitor %s: %s", html.EscapeString(monitorId), err))
		http.Error(w, errMsg, http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (api *API) monitorsStop(w http.ResponseWriter, r *http.Request) {
	blip.Debug("%v", r)
	monitorId, mon, ok := api.monitorId(w, r)
	if !ok {
		return // monitorId() wrote error response
	}
	blip.Debug("stop %s", monitorId)
	mon.Stop() // Stopy only returns nil
	w.WriteHeader(http.StatusOK)
}

// --------------------------------------------------------------------------
// Status endpoints
// --------------------------------------------------------------------------

func (api *API) status(w http.ResponseWriter, r *http.Request) {
	blip.Debug("%v", r)
	status.Blip("uptime", "%d", int(time.Since(api.startTs).Seconds()))
	json.NewEncoder(w).Encode(status.ReportBlip())
}

func (api *API) statusMonitors(w http.ResponseWriter, r *http.Request) {
	blip.Debug("%v", r)
	json.NewEncoder(w).Encode(status.ReportMonitors())
}

// --------------------------------------------------------------------------
// Helper funcs

// monitorId returns the monitor ID from URL query param '?id=monitorId' if set.
// Else it returns an empty string and false. The caller must return without
// doing anything else on false because this func writes the HTTP error response
// on false.
func (api *API) monitorId(w http.ResponseWriter, r *http.Request) (string, *monitor.Monitor, bool) {
	monitorId := r.URL.Query().Get("id")
	if monitorId == "" {
		http.Error(w, "missing id=monitorId in query", http.StatusBadRequest)
		return "", nil, false
	}
	mon := api.monitorLoader.Monitor(monitorId)
	if mon == nil {
		http.Error(w, fmt.Sprintf("monitor %s not loaded", html.EscapeString(monitorId)), http.StatusNotFound)
		return "", nil, false
	}
	// Avoid code scanning alert "Log entries created from user input"
	monitorId = strings.Replace(monitorId, "\n", "", -1)
	monitorId = strings.Replace(monitorId, "\r", "", -1)
	return monitorId, mon, true
}
