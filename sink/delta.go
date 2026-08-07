// Copyright 2024 Block, Inc.

package sink

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cashapp/blip/v2"
)

// The Delta sink calculates DELTER_COUNTER metrics from CUMULATIVE_COUNTER metrics. It acts as a transform,
// removing CUMULATIVE_COUNTER metrics and replacing them with DELTER_COUNTER values. This can be used
// to wrap sinks that expect counters to be submitted as the number/count of observations in the
// sampling interval rather than a cumulative total.
//
// The Delta sink is perferable to peforming delta calculations in the wrapped sink
// as the presence of a Retry sink can cause metrics to be sent to wrapped sink out of order, which
// can cause incorrect metric values to be submitted then delta calculations are performed.
// The Delta sink should never be wrapped inside of a Retry sink to prevent this.
type Delta struct {
	sink blip.Sink
	mu   sync.Mutex

	counters map[metricID]float64 // holds last value of the counter so deltas can be calculated
}

type metricID struct {
	domain string
	name   string
	group  string
}

var _ blip.Sink = &Delta{}

func NewDelta(sink blip.Sink) *Delta {
	if sink == nil {
		panic("sink is nil; value required")
	}
	if _, ok := sink.(*Delta); ok {
		panic("sink cannot be a Delta sink.")
	}

	return &Delta{
		sink:     sink,
		counters: make(map[metricID]float64),
	}
}

func (d *Delta) Name() string {
	return "delta"
}

// Calculates DELTA_COUNTER values from any CUMULATIVE_COUNTER values in
// the passed metircs, and then replacees the CUMULATIVE_COUNTER values
// with the new DELTA_COUNTER values. The updated metrics are forwarded
// to the next sink.
//
// This is safe to call from multiple goroutines.
func (d *Delta) Send(ctx context.Context, metrics *blip.Metrics) error {
	return d.sink.Send(ctx, d.transform(metrics))
}

func (d *Delta) transform(metrics *blip.Metrics) *blip.Metrics {
	// Protect counter state and transformation, but do not serialize the
	// wrapped sink: Retry depends on accepting a new batch while an earlier
	// batch is still in flight.
	d.mu.Lock()
	defer d.mu.Unlock()

	newValues := make(map[string][]blip.MetricValue)
	hasNewValues := false

	for key, collection := range metrics.Values {
		valueList := make([]blip.MetricValue, 0, len(collection))
		hasDelta := false

		for _, value := range collection {
			// Calculate a DELTA for any cumulative counters
			switch value.Type {
			case blip.CUMULATIVE_COUNTER:
				hasDelta = true
				metricValue := value.Value
				id := d.metricID(key, value.Name, value.Group)

				val, ok := d.counters[id]
				if !ok {
					// If we don't have a prior data point then we should
					// not calculate a delta and just remove the point.
					d.counters[id] = value.Value
					continue
				}

				delta := value.Value - val
				d.counters[id] = value.Value
				if delta >= 0 {
					metricValue = delta
				} else {
					blip.Debug("found negative delta for: %s (can happen due to restart), sending the potentially partial metric value", value.Name)
				}

				value.Value = metricValue
				value.Type = blip.DELTA_COUNTER
				break

			default:
				break
			}

			valueList = append(valueList, value)
		}

		if hasDelta {
			newValues[key] = valueList
			hasNewValues = true
		} else {
			// If we didn't have to calculate any deltas we can just reuse the existing array
			newValues[key] = collection
		}
	}

	if !hasNewValues {
		// If we didn't have to calculate any deltas then we should
		// just return the original metrics.
		return metrics
	}

	transformed := *metrics
	transformed.Values = newValues
	return &transformed
}

// metricID identifies one cumulative counter series. Group entries are sorted
// and length-prefixed so keys, values, and their boundaries are unambiguous.
func (d *Delta) metricID(domain, name string, groups map[string]string) metricID {
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}

	// sort by keys
	sort.Strings(keys)

	var group strings.Builder
	for _, k := range keys {
		value := groups[k]
		group.WriteString(strconv.Itoa(len(k)))
		group.WriteByte(':')
		group.WriteString(k)
		group.WriteString(strconv.Itoa(len(value)))
		group.WriteByte(':')
		group.WriteString(value)
	}

	return metricID{domain: domain, name: name, group: group.String()}
}
