---
title: datadog
---

{{< hint type=important >}}
Blip works with Datadog, but Datadog does not support or contribute to Blip.
{{< /hint >}}

The datadog sink sends metrics to [Datadog](https://www.datadoghq.com/).

This sink is an optional replacement for the [Datadog Agent for MySQL](https://docs.datadoghq.com/integrations/mysql/).
Consequently, the metrics it sends are [billed as custom metrics](https://docs.datadoghq.com/account_management/billing/custom_metrics/).

Metrics are reported as domain-qualified Blip metric names: `status.global.threads_running`.
All [tags]({{< ref "/config/config-file#tags" >}}) are reported as Datadog tags.

Metrics can be sent either to Datadog API directly or through a Datadog agent using DogStatsD protocol.
Only one of the methods can be used at a time.
If `dogstatsd-host` option is set, DogStatsD is used for sending metrics, otherwise `api` keys must be provided.
If the `doststatsd-host` doesn't include the port, `8125` is used as the default port.

## Counter Metrics
Previously, blip treated most counter metrics as cumulative counters during collection, except for those that were truncated after collection. Although certain metrics platforms support cumulative counters as a metric type, Datadog only supports delta as counter values.

In versions prior to v1.1, this sink would send cumulative counter values to Datadog, resulting in confusion and unexpected behavior with certain functionalities.

Starting from v1.1, this sink now sends delta values to Datadog for counter metrics. As a result, Datadog queries and functions should work as expected.

If you wish to track the difference in counter values since v1.1, you can tag metrics with the blip version.

This sink also accepts the [retry]({{< ref "retry" >}}) options `buffer-size`, `send-timeout`, and `send-retry-wait`.

## Quick Reference

```yaml
sinks:
  datadog:
    api-compress: "true"
    api-key-auth: ""
    api-key-auth-file: ""
    app-key-auth: ""
    app-key-auth-file: ""
    metric-translator: ""
    metric-prefix: ""
    dogstatsd-host: ""
```
