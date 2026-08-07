---
weight: 0
---

## v2.0

This is a new major version. It added new metric domains, runtime debug controls, extensible AWS Secrets Manager parsing, and a more resilient Datadog delivery path.

As described in the [Blip versioning guidelines](https://github.com/block/blip/blob/main/CONTRIBUTING.md#versioning), this series is not entirely backwards-compatible with v1.2. Integrations that use the affected exported APIs must be updated before upgrading.

### Integration API changes

|Component|v1.2|v2.0|
|---------|----|----|
|AWS secret password helper|`aws.Secret.Password(context.Context) (string, error)`|Removed; use `GetSecret` or `GetSecretPayload`|
|Database credential type|`dbconn.Credentials`|`blip.DbCredentials`|
|Database credential callback|Returned `dbconn.Credentials`|Returns `blip.DbCredentials`|
|Default connection factory|`NewConnFactory(awsConfig, modifyDB)`|`NewConnFactory(awsConfig, modifyDB, ...ConnFactoryOption)`|
|Database-size query helper|Returned `(string, error)`|Returns `(string, []interface{}, error)`|
|Table-size query helper|Returned `(string, error)`|Returns `(string, []interface{}, error)`|
|Table-I/O query helper|Returned `string`|Returns `(string, []interface{})`|
|`heartbeat.BlipReader` values|Comparable|Not comparable|

To upgrade an integration:

1. Replace `dbconn.Credentials` with `blip.DbCredentials` and update any `dbconn.CredentialFunc` implementations.
2. Accept the new variadic options argument when storing or wrapping `dbconn.NewConnFactory`; ordinary two-argument calls continue to compile.
3. Capture the parameter slice returned by `DataSizeQuery`, `TableSizeQuery`, and `TableIoWaitQuery`, and pass it to the database query call.
4. Replace `aws.Secret.Password` calls with `GetSecret` for the default JSON object or `GetSecretPayload` plus a password secret parser for custom payloads.
5. Stop comparing `heartbeat.BlipReader` values directly; compare the relevant state exposed by the reader instead.

### Runtime changes

AWS Secrets Manager `password-secret` authentication now uses the secret's optional string `username` value instead of always using the configured monitor username. Remove `username` from the secret to retain the v1.2 behavior.

Default sink HTTP clients now have a 10-second whole-request timeout plus bounded connection and response-header timeouts. Custom HTTP client factories are unchanged.

### v2.0.0 (7 Aug 2026)

* Added the `autoinc` domain for auto-increment column utilization.
* Added the `error.account`, `error.global`, `error.host`, `error.thread`, and `error.user` domains.
* Added the `innodb.buffer-pool` domain.
* Added runtime debug toggling through `GET /debug` and `SIGUSR1`.
* Added customizable parsing of AWS Secrets Manager `SecretString` and `SecretBinary` payloads.
* Added bounded and checkpointed Datadog payload submission to avoid oversized requests and resume partially acknowledged batches.
* Prevented sink requests from blocking metric delivery indefinitely.
* Preserved complete metric metadata and isolated counter state in the delta sink.
* Redacted database, sink, and authenticated proxy credentials from debug logs.
* Fixed query construction to use driver parameter interpolation across collectors and heartbeats.
* Fixed nondeterministic `query.response-time` bucket selection and added detailed latency diagnostics.
* Fixed a shutdown panic in signal handling.
* Updated the test matrix from MySQL 5.7 to MySQL 8.4 and refreshed dependencies.

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
