// Copyright 2024 Block, Inc.

package status

import "sync/atomic"

// Monitor counters are cumulative counts of collection faults: conditions
// where a monitor collected late, partially, or dropped metrics. Each counter
// only increases, and it persists across monitor stop and start; it's reset
// only when the monitor is unloaded, which includes reloading a monitor after
// its config changes. Counters are reported once the monitor starts. A host
// can read counters with ReportCounters (or API endpoint
// /status/monitors/counters) and forward them to its own metrics system.
// Compare successive values to get a rate.
//
// The engine and collector counters (ENGINE_* and COLLECTOR_*) count level
// collector (LCO) collections. If the monitor runs an exporter, its engine
// counts scrapes in the same counters with prefix EXPORTER_PREFIX, like
// "exporter-engine-emr-timeouts".
const (
	// Domains not started before the engine max runtime (EMR) expired.
	ENGINE_DOMAINS_NOT_STARTED = "engine-domains-not-started"

	// Domains started but not returned before the EMR expired. Their metrics
	// are missing from the interval; they're reported late if the domain
	// collector returns before its own max runtime (CMR).
	ENGINE_DOMAINS_NOT_RETURNED = "engine-domains-not-returned"

	// Engine collections (one per interval) that hit the EMR before every
	// domain was started and returned. This usually means the host is
	// CPU-starved, but a domain collector that hangs (COLLECTOR_FAULTS) or
	// panics in its foreground Collect (COLLECTOR_PANICS) doesn't return, so
	// it also causes EMR timeouts.
	ENGINE_EMR_TIMEOUTS = "engine-emr-timeouts"

	// Domain collectors still running at their next interval because they
	// didn't stop at their CMR (event COLLECTOR_FAULT). The previous interval's
	// metrics from the domain are dropped (event DROP_METRICS_FENCE) if the
	// collector ever returns.
	COLLECTOR_FAULTS = "collector-faults"

	// Domain collectors that panicked (event COLLECTOR_PANIC). The domain's
	// metrics for the interval are lost. A panic in the foreground Collect of
	// the current interval isn't flushed, so the engine waits for the domain
	// until the EMR, and the panic is also counted in ENGINE_EMR_TIMEOUTS and
	// ENGINE_DOMAINS_NOT_RETURNED. A panic in the ErrMore background loop, or
	// in a run behind the collector fault fence, doesn't change those counters.
	COLLECTOR_PANICS = "collector-panics"

	// Domain collectors that returned context.DeadlineExceeded because they
	// reached their collector max runtime (CMR). Their metrics are partial or
	// missing (event COLLECTOR_ERROR if returned within the EMR). Only counted
	// if the collector returns the context error or wraps it with %w. On a
	// CPU-starved host, this rises almost one for one with ENGINE_EMR_TIMEOUTS.
	COLLECTOR_TIMEOUTS = "collector-timeouts"

	// Domain metrics dropped because the engine collection channel was full
	// (event DROP_METRICS_FLUSH).
	COLLECTOR_FLUSH_DROPS = "collector-flush-drops"

	// Level collector ticks dropped because the previous collection or plan
	// change blocked the level collector loop past the next tick. Each dropped
	// tick delays every later level by one tick.
	LEVEL_TICKS_DROPPED = "level-ticks-dropped"

	// Collections dropped because the sink channel was full: sinks are slower
	// than collection (event LCO_METRICS_FAULT). Each collection is the metrics
	// for one interval plus any late metrics from earlier intervals, so a drop
	// can lose metrics from more than one interval.
	LEVEL_METRICS_DROPPED = "level-metrics-dropped"

	// Collections dropped because the TransformMetrics plugin returned an
	// error. Metrics lost when a sink or TransformMetrics panics (event
	// LCO_RECEIVER_PANIC) are not counted.
	LEVEL_TRANSFORM_DROPPED = "level-transform-dropped"

	// Metrics dropped from a sink retry buffer because it was full: the oldest
	// buffered metrics are pushed off to make room for the latest. The counter
	// name is suffixed with ":" and the sink name, like "sink-retry-dropped:datadog".
	SINK_RETRY_DROPPED = "sink-retry-dropped"

	// Prefix of the engine and collector counters of the exporter engine.
	EXPORTER_PREFIX = "exporter-"
)

// Counter returns the named counter for the monitor, creating it at zero if it
// doesn't exist. Callers should get the counter once when the monitor starts
// and increment it with Add(1): increments are lock-free and do not allocate.
// The returned pointer is reported until RemoveMonitor is called. The monitor
// loader calls RemoveMonitor when it unloads a monitor, including a changed
// monitor after it makes the replacement, so don't get counters when making
// monitor components that outlive a start, like sinks (see sink.CounterSink).
func Counter(monitorId, name string) *atomic.Uint64 {
	s.Lock()
	defer s.Unlock()
	m, ok := s.counters[monitorId]
	if !ok {
		m = map[string]*atomic.Uint64{}
		s.counters[monitorId] = m
	}
	c, ok := m[name]
	if !ok {
		c = &atomic.Uint64{}
		m[name] = c
	}
	return c
}

// ReportCounters returns the current value of all monitor counters keyed on
// monitor ID, then counter name. If ids are given, only those monitors are
// reported. Counters are created at zero by the components that use them, so
// a counter can be reported as zero before it has ever incremented.
func ReportCounters(ids ...string) map[string]map[string]uint64 {
	s.Lock()
	defer s.Unlock()
	var allow map[string]bool
	if len(ids) > 0 {
		allow = map[string]bool{}
		for _, id := range ids {
			allow[id] = true
		}
	}
	report := map[string]map[string]uint64{}
	for monitorId, counters := range s.counters {
		if len(allow) > 0 && !allow[monitorId] {
			continue
		}
		report[monitorId] = make(map[string]uint64, len(counters))
		for name, c := range counters {
			report[monitorId][name] = c.Load()
		}
	}
	return report
}
