---
---

Blip does not have a single format or protocol for reporting metrics.
Instead, after [collecting metrics](collecting) Blip uses metric [sinks](../sinks/) to translate the Blip [metric data structure](#metric-data-structure) to a sink-specific protocol.

Normally, a sink sends metrics somewhere, but a sink can do anything with the metrics&mdash;Blip does not impose any restrictions on sinks.
For example, the default [log sink](../sinks/log) dumps metrics to `STDOUT`.

Blip has several [built-in sinks](../sinks/), but unless you happen to use one of them, you will need to write a [custom sink](../develop/sinks) to make Blip report metrics in your environment.
Start by understanding how Blip reports metrics internally (as detailed on this page), then read [Develop / Sinks](../develop/sinks).

{{< toc >}}

## Metrics

### Types

Blip metric types are standard:

* `COUNTER`
* `GAUGE`
* `BOOL`
* `EVENT` (reserved for future use)

Blip automatically uses the correct metric type for all metrics.

Blip, like MySQL, does not distinguish between "counter" and "cumulative counter".
Blip counter metrics can reset to zero if MySQL is restarted; otherwise, the value only increases.

### Values

All values, regardless of type, are `float64`.

Negative values are allowed.
Some [derived metrics](collecting#derived-metrics) use negative values as documented in the [domain reference](domains).
MySQL metrics are not supposed to be negative, but there are MySQL bugs that cause negative values.

### Units

MySQL metrics use a variety of units&mdash;from picoseconds to seconds.
When the MySQL metric unit is documented and consistent, Blip reports the value as-is.
For example, `innodb.buffer_flush_avg_time` is documented as "Avg time (ms) spent for flushing recently.", therefore Blip reports the value as-is: as milliseconds.

When the MySQL metric unit is variable, Blip uses the following units:

|Metric Type|Unit|
|-----------|----|
|Query time|microseconds (μs)
|Lock time|microseconds (μs)
|Wait time|microseconds (μs)
|Replication (lag)|milliseconds (ms)
|Data size|bytes

For example, query response time ranges from nanoseconds to seconds (with microsecond precision) depending on the source.
But regardless of the source, Blip reports query time as _microseconds (μs)_.

{{< hint type=tip >}}
To convert units, use the [TransformMetrics plugin](../develop/integration-api#plugins) or write a [custom sink](../develop/sinks)
{{< /hint >}}

Blip does _not_ suffix metric names with units, and it does not strip the few MySQL metrics that have unit suffixes.

### Renaming

Blip never renames MySQL metrics on collection or within its [metric data structure](#metric-data-structure).
Metrics can be renamed _after_ collection by using the [TransformMetrics plugin](../develop/integration-api#plugins) or writing a [custom sink](../develop/sinks).

## Metric Data Structure

Internally, Blip stores metrics in a [`Metrics` data structure](https://pkg.go.dev/github.com/cashapp/blip/v2#Metrics):

```
type Metrics struct {
	Begin     time.Time                // when collection started
	End       time.Time                // when collection completed
	MonitorId string                   // ID of monitor (MySQL)
	Plan      string                   // plan name
	Level     string                   // level name
	State     string                   // state of monitor
	Values    map[string][]MetricValue // keyed on domain
}
```

[Metric values](https://pkg.go.dev/github.com/cashapp/blip/v2#MetricValue) (the last field in the struct) are reported per-domain.
To visual the metric values data structure, in YAML it would be:

```yaml
status.global: # domain

  - name:  queries
    value: 5382      
    type:  1 # counter
    group:
      key1: "val1"
    meta:
      key1: "val1"

  - name: threads_running
    value: 15
    type:  2 # gauge
```

{{< hint type=important >}}
Blip metrics are not stored or reported in YAML. Examples are for illustration only.
{{< /hint >}}

All metrics belong to a single [domain](domains), and each domain can collect any number of metrics.

All metrics have a name, type, and value.
Blip automatically sets the type and value of every metric.
The name is set by MySQL (for [MySQL metrics](collecting#mysql-metrics)) or Blip (for [derived metrics](collecting#derived-metrics)).
Blip lowercases all metric names, even MySQL metric names.

Some metrics have [groups](#groups) and [meta](#meta).

Metric [sinks](../sinks) convert the metric data structures to a sink-specific data structure.
For example, [Prometheus emulation](../prometheus) acts as a pseudo-sink to convert and report Blip metrics in [Exposition format](https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md):

```
# TYPE mysql_global_status_threads_running gauge
mysql_global_status_threads_running 16
# TYPE mysql_global_status_queries counter
mysql_global_status_queries 138923
```

After Blip sends metrics to a sink, the sink fully owns the metrics; Blip no longer references the metrics.

### Groups

Certain [domains](domains) set group key-value pairs to distinguish different metrics with the same name.
For example, the [`size.database` domain](domains#sizedata) groups metrics by database name.
Each metric is grouped on `db`:

```yaml
size.database:
  - bytes:
      value: 50920482048
      type: 2 # gauge
      group:
        db: "foo"
  - bytes:
      value: 8920482048
      type: 2 # gauge
      group:
        db: "bar"
  - bytes:
      value: 59840964096
      type: 2 # gauge
      group:
        db: "" # all databases
```

The metric name is the same&mdash;`bytes`&mdash;but there are 3 instances of the metric for each of the 3 groups: `db=foo`, `db=bar`, and `db=""`.
The last group, `db=""`, is how this domain represents the metric for all databases (the global group).

<mark>When set, groups are required to uniquely identify metrics.</mark>
In the example above, the derived metric name `bytes` does not unique identify a metric; you must include the group keys.
How the group keys are included is sink-specific, but most translate the groups to tags, labels, or dimensions.

{{< hint type=note >}}
_Groups_, _labels_, and _dimensions_ serve the same purpose.
Blip uses the term _group_ because it's similar to MySQL `GROUP BY`.
{{< /hint >}}

### Meta

Certain [domains](domains) set metadata about metrics as documented.
An example is [`query.response-time`](domains#queryresponse-time):

```yaml
query.response-time:
  - p95:
      value: 500.2
      type: 2 # gauge
      meta:
        p95: "95.8"
```

The domain collects the P95 query response time value, but due to how MySQL calculates percentiles, it might not be the exact P95.
In the metadata, the collector adds the collected percentile (`p95`) and the real percentile value: 95.8.

Another example is [`repl`](domains#repl): it sets meta key `source` equal to `Source_Host` (or `Master_Host`) from `SHOW REPLICA STATUS`.

Unlike [groups](#groups), meta does _not_ uniquely identify metrics and is optional.
