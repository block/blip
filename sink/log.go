// Copyright 2024 Block, Inc.

package sink

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cashapp/blip/v2"
)

// Sink logs metrics.
type logSink struct {
	monitorId string
}

func NewLogSink(monitorId string) (logSink, error) {
	return logSink{monitorId: monitorId}, nil
}

func (s logSink) Send(ctx context.Context, m *blip.Metrics) error {
	fmt.Printf("# monitor:  %s\n", m.MonitorId)
	fmt.Printf("# plan:     %s\n", m.Plan)
	fmt.Printf("# level:    %s\n", m.Level)
	fmt.Printf("# ts:       %s\n", m.Begin.Format(time.RFC3339Nano))
	fmt.Printf("# duration: %d ms\n", m.End.Sub(m.Begin).Milliseconds())
	for domain, values := range m.Values {
		for i := range values {
			metricStr := fmt.Sprintf("%s.%s = %d", domain, values[i].Name, int64(values[i].Value))
			if len(values[i].Group) > 0 {
				metricStr = fmt.Sprintf("%s (group: %s)", metricStr, sortedTuples(values[i].Group))
			}
			if len(values[i].Meta) > 0 {
				metricStr = fmt.Sprintf("%s (meta: %s)", metricStr, sortedTuples(values[i].Meta))
			}
			fmt.Println(metricStr)
		}
	}
	fmt.Println()
	return nil
}

func (s logSink) Status() string {
	return "swimmingly"
}

func (s logSink) Name() string {
	return "log"
}

func sortedTuples(m map[string]string) string {
	// Tuples sorted by keys to avoid this:
	//  size.table.bytes = 32768 (group: db=test,tbl=t2)
	//  size.table.bytes = 16384 (group: tbl=q1,db=test)
	//  size.table.bytes = 32768 (group: db=test,tbl=t3)
	var tuples []string
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	for _, k := range keys {
		tuples = append(tuples, fmt.Sprintf("%s=%s", k, m[k]))
	}
	return strings.Join(tuples, ",")
}
