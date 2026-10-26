// Copyright 2024 Block, Inc.

package server

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cashapp/blip/v2/monitor"
)

func TestWriteStartResult(t *testing.T) {
	// A monitor that a reload removes after api.monitorId found it isn't
	// loaded when TryStart runs: that's 404, like any monitor that isn't loaded.
	tests := []struct {
		name string
		res  monitor.StartResult
		err  error
		code int
	}{
		{"started", monitor.START_OK, nil, http.StatusOK},
		{"pending", monitor.START_PENDING, nil, http.StatusAccepted},
		{"running", monitor.START_RUNNING, nil, http.StatusConflict},
		{"declined", monitor.START_DECLINED, nil, http.StatusConflict},
		{"not loaded", monitor.START_ERROR, monitor.ErrMonitorNotLoaded, http.StatusNotFound},
		{"error", monitor.START_ERROR, errors.New("start failed"), http.StatusConflict},
	}
	for _, tt := range tests {
		w := httptest.NewRecorder()
		writeStartResult(w, "m1", tt.res, tt.err)
		if w.Code != tt.code {
			t.Errorf("%s: got %d, expected %d", tt.name, w.Code, tt.code)
		}
	}
}
