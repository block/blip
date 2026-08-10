// Copyright 2024 Block, Inc.

package server

import (
	"net/http"
	"testing"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/stretchr/testify/require"
)

func TestHTTPClientFactoryMakeForSink(t *testing.T) {
	tests := []struct {
		name  string
		proxy string
	}{
		{name: "default"},
		{name: "proxy", proxy: "http://proxy.example:8080"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := httpClientFactory{cfg: blip.ConfigHTTP{Proxy: tt.proxy}}
			client, err := factory.MakeForSink("datadog", "m1", nil, nil)
			require.NoError(t, err)
			require.Equal(t, 10*time.Second, client.Timeout)

			transport, ok := client.Transport.(*http.Transport)
			require.True(t, ok)
			require.True(t, transport.ForceAttemptHTTP2)
			require.NotNil(t, transport.DialContext)
			require.Equal(t, 5*time.Second, transport.TLSHandshakeTimeout)
			require.Equal(t, 5*time.Second, transport.ResponseHeaderTimeout)
			require.Equal(t, 1*time.Second, transport.ExpectContinueTimeout)
			require.Equal(t, 30*time.Second, transport.IdleConnTimeout)

			otherClient, err := factory.MakeForSink("datadog", "m2", nil, nil)
			require.NoError(t, err)
			require.NotSame(t, client.Transport, otherClient.Transport)

			if tt.proxy == "" {
				return
			}

			request, err := http.NewRequest(http.MethodGet, "https://api.datadoghq.com", nil)
			require.NoError(t, err)
			proxyURL, err := transport.Proxy(request)
			require.NoError(t, err)
			require.Equal(t, tt.proxy, proxyURL.String())
		})
	}
}
