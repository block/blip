// Copyright 2026 Block, Inc.

package sink

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/status"
)

type testHTTPClientFactory struct {
	opts map[string]map[string]string // sink name => options
}

func (f *testHTTPClientFactory) MakeForSink(sinkName, monitorId string, opts, tags map[string]string) (*http.Client, error) {
	f.opts[sinkName] = opts
	return &http.Client{}, nil
}

func TestFactoryMake_RetryOptions(t *testing.T) {
	httpFactory := &testHTTPClientFactory{opts: map[string]map[string]string{}}
	defer func(prev blip.HTTPClientFactory) { f.HTTPClient = prev }(f.HTTPClient)
	f.HTTPClient = httpFactory

	retryOpts := map[string]string{
		"buffer-size":     "3",
		"send-timeout":    "7s",
		"send-retry-wait": "300ms",
	}

	tests := []struct {
		sinkName string
		opts     map[string]string // sink-specific options
		http     bool              // uses HTTPClient.MakeForSink
	}{
		{sinkName: "chronosphere", opts: map[string]string{"url": "http://127.0.0.1:3030"}},
		{sinkName: "signalfx", opts: map[string]string{"auth-token": "token"}, http: true},
		{sinkName: "datadog", opts: map[string]string{"api-key-auth": "key", "app-key-auth": "key"}, http: true},
		{sinkName: "prom-pushgateway", opts: map[string]string{"addr": "http://127.0.0.1:9091"}},
	}
	for _, tt := range tests {
		t.Run(tt.sinkName, func(t *testing.T) {
			opts := map[string]string{}
			for k, v := range tt.opts {
				opts[k] = v
			}
			for k, v := range retryOpts {
				opts[k] = v
			}

			monitorId := "factory-retry-" + tt.sinkName
			defer status.RemoveMonitor(monitorId)

			s, err := Make(blip.SinkFactoryArgs{
				SinkName:  tt.sinkName,
				MonitorId: monitorId,
				Options:   opts,
			})
			require.NoError(t, err)

			// Monitor registers the retry buffer counter through the returned sink
			cs, ok := s.(CounterSink)
			require.True(t, ok, "sink is %T, expected CounterSink", s)
			cs.RegisterCounters()
			counter := status.SINK_RETRY_DROPPED + ":" + tt.sinkName
			_, ok = status.ReportCounters(monitorId)[monitorId][counter]
			assert.True(t, ok, "%s not reported", counter)

			if d, ok := s.(*Delta); ok {
				s = d.sink
			}
			retry, ok := s.(*Retry)
			require.True(t, ok, "sink is %T, expected *Retry", s)
			assert.Equal(t, 3, retry.max+1)
			assert.Equal(t, 3, len(retry.stack))
			assert.Equal(t, 7*time.Second, retry.sendTimeout)
			assert.Equal(t, 300*time.Millisecond, retry.retryWait)
			assert.Equal(t, tt.sinkName, retry.Name())

			// Caller's options are not modified
			assert.Len(t, opts, len(tt.opts)+len(retryOpts))

			if tt.http {
				assert.Equal(t, tt.opts, httpFactory.opts[tt.sinkName])
			}
		})
	}
}

func TestFactoryMake_RetryDefaults(t *testing.T) {
	s, err := Make(blip.SinkFactoryArgs{
		SinkName:  "chronosphere",
		MonitorId: "m1",
	})
	require.NoError(t, err)

	retry, ok := s.(*Retry)
	require.True(t, ok, "sink is %T, expected *Retry", s)
	assert.Equal(t, DEFAULT_RETRY_BUFFER_SIZE, retry.max+1)
	assert.Equal(t, 5*time.Second, retry.sendTimeout)
	assert.Equal(t, 200*time.Millisecond, retry.retryWait)
}
