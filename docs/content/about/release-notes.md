---
weight: 0
---

## v1.2

This is a new series (new minor version).
A quick summary of what's new in this series:

* "Engine v2": monitor.Engine was rewritten to better handle long-running collectors, which simplifies delta counter handling in the new sink.Delta.
Previously, two levels could run in parallel because metrics were originally designed and intended to be stateless.
But delta counters are implicitly stateful: interval 1 is required before interval 2 in order to calculate the delta.
Serial level collection solves the order problem but then requires new handling for long-running domains.
Engine v2 handles all collection cases (normal and edge) correctly and consistently.

As per the [Blip versioning guidelines](https://github.com/cashapp/blip/blob/main/CONTRIBUTING.md#versioning), this new series is ***not*** entirely backwards-compatible with v1.1 due to these changes:

|# |Component|v1.0|v1.1|
|--|---------|----|----|
|1 |`blip.Plugins`|`TransformMetrics func(*Metrics)`|`TransformMetrics func([]*Metrics) error`|
|2 |Events|See below|See below|

How to upgrade (by number in the table above):

1. The first argument changed from one `*blip.Metrics` to a slice of metrics, and it returns an error.
Update your `TransformMetrics` plugin function to match, and you'll most likely wrap its original logic in a `for` loop, like:

```go
func(metrics []*blip.Metrics) error {

    for _, m := range metrics {
        /* Original logic */
    }
    return nil
}
```

2. If using an integration that works with Blip events, see [event/list.go](https://github.com/cashapp/blip/blob/main/event/list.go) for the new event names.

### v1.2.1 (19 Jul 2024)

* Fixed bug (panic) in `monitor/level_collector` when plan has no levels.
* Added `plan/default.None`.

### v1.2.0 (2 Jul 2024)

* Rewrote monitor.Engine ("engine v2") and some of level collector (LCO)
  * Removed parallel level collection; made level collection serial
  * Fixed long-running domain handling
  * Added collector max runtime (CMR) context _per domain_ equal to minimum level frequency
  * Added [`ErrMore`](https://block.github.io/blip/develop/collectors/#long-running)
  * Added collector fault fencing: collector and its results are fenced off (dropped) if non-responsive or returns too late
  * Added domain priority: collectors are started by ascending domain frequency (e.g. 5s domain collectors start before 20s domain collectors)
* Added `blip.Metrics.Interval` field
* Added `sink.Delta` wrapper for automatic/transparent delta counter handling
* Removed multi-component status
* Renamed and added several events
* Changed `blip.Plugins.TransformMetrics`
* Changed testing default from MySQL 5.7 to MySQL 8.0

---

## v1.1

This is a new series (new minor version).
A quick summary of what's new in this series:

* The Datadog sink sends delta counters, which is what Datadog expects.
* The repl.lag collector defaults to MySQL 8.x Performance Schema replication tables.

As per the [Blip versioning guidelines](https://github.com/cashapp/blip/blob/main/CONTRIBUTING.md#versioning), this new series is ***not*** entirely backwards-compatible with v1.0 due to these changes:

|# |Component|v1.0|v1.1|
|--|---------|----|----|
|1 |`datadog` sink|Sends cumulative counters|Sends delta counters|
|2 |`repl.lag` collector|Default `writer=blip`|Default `writer=auto` will use Performance Schema on MySQL 8.x|

How to upgrade (by number in the table above):

1. Datadog counters are deltas by default, so the new behavior works better. Use [`.as_rate()`](https://docs.datadoghq.com/metrics/custom_metrics/type_modifiers/?tab=count) instead of the [`rate()` function](https://docs.datadoghq.com/dashboards/functions/rate/). Note that Datadog charts work best when the interval is set for each metric.
2. To continue using Blip heartbeats, explicitly configure `repl.lag` with option `writer=blip`.

### v1.1.0 (17 Jun 2024)

* `datadog` sink:
  * Changed to send delta counters instead of cumulative counters (PR #106)
* `repl.lag` collector:
  * Added MySQL 8.x Performance Schema support (auto-detected or writer=pfs) (PR #118)
* `wait.io.table` collector:
  * Added `count_star` to metrics
* Added sink `prom-pushgateway` ([Prometheus Pushgateway](https://github.com/prometheus/pushgateway))
* Updated built-in AWS RDS CA from rds-ca-2019 to the global bundle (PR #113)
* Made HA manager a configurable plugin (PR #116)
* Changed `max_used_connetions` to gauge (PR #111)
* Fixed GitHub Dependabot alerts

---

## v1.0

### v1.0.2 (03 Jul 2023)

* `datadog` sink:
  * Fixed timestamps: DD expects timestamp as seconds, not milliseconds
  * Send new `event.SINK_ERROR` and debug DD API errors on successful request
* `query.response-time` and `wait.io.table` collectors:
  * Added `truncate-timeout` option and error policy
  * Fixed docs: option `truncate-table` defaults to "yes"
* Fixed GitHub Dependabot alerts

### v1.0.1 (02 Mar 2023)

* `datadog` sink:
  * Fixed intermittent panic
  * Fixed HTTP error 413 (payload too large) by dynamically partitioning metrics
  * Added option `api-compress` (default: yes)
* `repl` collector:
  * Added option `report-not-a-replica`
  * Moved pkg vars `statusQuery` and `newTerms` to `Repl` to handle multiple collectors on different versions
  * Fixed docs (only `repl.running` is currently collected)
* Updated `aws/AuthToken.Password`: pass context to `auth.BuildAuthToken`
* Fixed GitHub Dependabot alerts
* Fixed `blip.VERSION`

### v1.0.0 (22 Dec 2022)

* First GA, production-ready release.
