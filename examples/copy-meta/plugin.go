package blip_plugin

import (
	"fmt"

	"github.com/cashapp/blip/v2"
)

// Verify that CopyMeta is a valid blip.Plugins.TransformMetrics callback.
var _ = blip.Plugins{
	TransformMetrics: CopyMeta,
}

func CopyMeta(metrics []*blip.Metrics) error {
	for _, m := range metrics {
		// If these metrics don't have repl.lag, that's ok: it's not the level that
		// collects repl.lag
		lagMetrics, ok := m.Values["repl.lag"]
		if !ok {
			continue
		}

		// This level has repl.lag, so it should have repl metrics, too
		replMetrics, ok := m.Values["repl"]
		if !ok {
			return fmt.Errorf("repl not collected with repl.lag at level %s", m.Level)
		}

		// Use first source meta value set in repl metrics
		source := ""
		for i := range replMetrics {
			if len(replMetrics[i].Meta) == 0 {
				continue
			}
			source, ok = replMetrics[i].Meta["source"]
			if ok {
				break
			}
		}
		if source == "" {
			return fmt.Errorf("no repl metrics have source meta value")
		}

		// Copy source from repl into all repl.lag metrics. This overwrites the
		// source meta value set by repl.lag.
		for i := range lagMetrics {
			if lagMetrics[i].Meta == nil {
				lagMetrics[i].Meta = map[string]string{}
			}
			lagMetrics[i].Meta["source"] = source
		}
	}

	return nil
}
