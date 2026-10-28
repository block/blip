---
---

Status endpoints return real-time status of Blip and monitors in key-value maps with string values, except [monitor counters](#get-statusmonitorscounters), which are integers.

{{< toc >}}

## GET /status

Returns high-level Blip server status.

### Response

```json
{
  "monitor-loader": "1 monitors started at 2022-11-28T20:26:49-05:00",
  "monitors": "1",
  "server": "running since 2022-11-28T20:26:49-05:00",
  "started": "2022-11-28T20:26:49-05:00",
  "uptime": "22",
  "version": "1.0.75"
}
```

## GET /status/monitors

Returns monitor status for all monitors keyed on [monitor ID]({{< ref "/config/config-file#id" >}}).

### Response

```json
{
  "localhost": {
    "dsn": "blip:...@unix(/tmp/mysql.sock)/?parseTime=true",
    "engine-plan": "default-mysql",
    "heartbeat-reader": "22 ms lag from node2 (), next in 1s",
    "level-collect": "last collected and sent metrics for default-mysql/performance at 2022-11-28T20:37:03-05:00 in 2.249204ms",
    "level-collector": "idle; started collecting default-mysql/performance at 2022-11-28T20:37:03-05:00",
    "level-plan": "default-mysql",
    "level-state": "active",
    "monitor": "running since 2022-11-28T20:36:57-05:00"
  }
}
```

## GET /status/monitors/counters

Returns collection fault counters for all monitors keyed on [monitor ID]({{< ref "/config/config-file#id" >}}), then counter name.
Counters count conditions where a monitor collected late, collected partial metrics, or dropped metrics.
Each counter only increases, and it persists across monitor stop and start; it's reset only when the monitor is unloaded, which includes reloading a monitor after its config changes.
Counters are reported once the monitor starts.
Compare successive values to get a rate.

|Counter|Increments once for every|
|-------|---------|
|`engine-emr-timeouts`|Collection (interval) that hit the engine max runtime (EMR) before every domain started and returned; usually a CPU-starved host, but a collector that hangs, or panics in its foreground collect, also causes EMR timeouts|
|`engine-domains-not-started`|Domain not started before the EMR|
|`engine-domains-not-returned`|Domain started but not returned before the EMR; its metrics are reported late, if at all|
|`collector-faults`|Domain still running at its next interval because it didn't stop at its collector max runtime (event `collector-fault`); its previous metrics are dropped|
|`collector-panics`|Domain collector panic (event `collector-panic`); its metrics for the interval are lost. A panic in the foreground collect of the current interval is also counted in `engine-emr-timeouts` and `engine-domains-not-returned`; a panic while collecting in the background (after returning `ErrMore`) or after a collector fault is not|
|`collector-timeouts`|Domain collector that stopped at its collector max runtime (CMR) and returned partial or no metrics. Only counted if the collector returns the context error or wraps it with `%w`; for example, `trx` and `stmt.current` don't yet, so their CMR timeouts are not counted|
|`collector-flush-drops`|Domain metrics dropped because the engine collection channel was full (event `drop-metrics-flush`)|
|`level-ticks-dropped`|Level collector tick dropped because the previous collection blocked past it; each delays later levels by one tick|
|`level-metrics-dropped`|Collection dropped because sinks were not keeping up (event `lco-metrics-fault`), which can include late metrics from more than one interval|
|`level-transform-dropped`|Collection dropped because the `TransformMetrics` plugin returned an error|
|`sink-retry-dropped:<sink>`|Metrics dropped from a sink retry buffer because it was full|

If the monitor runs an [exporter]({{< ref "/config/config-file#exporter" >}}), its scrapes are counted in the `engine-*` and `collector-*` counters prefixed with `exporter-`, like `exporter-engine-emr-timeouts`.
The unprefixed counters count only level collector collections.

Metrics lost when a sink or `TransformMetrics` panics (event `lco-receiver-panic`) are not counted.

When the host running Blip is CPU-starved, the `engine-*` and `level-ticks-dropped` counters increase first.
Collectors then run out of time too, so `collector-timeouts` rises almost one for one with `engine-emr-timeouts`.
`level-ticks-dropped` increases only when the level collector itself falls behind, so it's the counter to watch for CPU starvation.
When a sink is slow or unavailable, `sink-retry-dropped` and then `level-metrics-dropped` increase.

Integrations can read the same counters with `status.ReportCounters`.

### Response

```json
{
  "localhost": {
    "collector-faults": 0,
    "collector-flush-drops": 0,
    "collector-panics": 0,
    "collector-timeouts": 1,
    "engine-domains-not-returned": 3,
    "engine-domains-not-started": 1,
    "engine-emr-timeouts": 3,
    "level-metrics-dropped": 0,
    "level-ticks-dropped": 2,
    "level-transform-dropped": 0,
    "sink-retry-dropped:datadog": 0
  }
}
```
