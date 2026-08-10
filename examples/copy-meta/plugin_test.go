package blip_plugin

import (
	"testing"

	"github.com/cashapp/blip/v2"
)

const (
	heartbeat_source = "01"
	immediate_source = "02"
)

func TestCopy(t *testing.T) {
	m := &blip.Metrics{
		MonitorId: "03",
		Plan:      "plan1",
		Level:     "level1",
		Values: map[string][]blip.MetricValue{
			"repl": {
				{
					Name:  "running",
					Type:  blip.GAUGE,
					Value: 1,
					Meta: map[string]string{
						"source": immediate_source, // This is copied...
					},
				},
			},
			"repl.lag": {
				{
					Name:  "current",
					Type:  blip.GAUGE,
					Value: 75,
					Meta: map[string]string{
						"source": heartbeat_source, // to here
					},
				},
			},
		},
	}

	err := CopyMeta([]*blip.Metrics{m})
	if err != nil {
		t.Fatal(err)
	}

	if m.Values["repl.lag"][0].Meta["source"] != immediate_source {
		t.Errorf("repl.lag.meta.source = %s, expected %s", m.Values["repl.lag"][0].Meta["source"], immediate_source)
	}
}
