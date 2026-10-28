---
title: chronosophere
---

{{< hint type=important >}}
Blip works with Chronosphere, but Chronosphere does not support or contribute to Blip.
{{< /hint >}}

{{< hint type=warning >}}
The chronosphere sink is experimental.
It might be removed in future versions.
{{< /hint >}}

The chronosphere sink sends metrics to [Chronosphere](https://chronosphere.io/).

It _pushes_ metrics to a Chronosphere collector.
If you want a Chronosphere collector to scrape metrics, use [Prometheus emulation]({{< ref "config/prometheus" >}}) instead.

This sink reports Prometheus-style metric names: `mysql_status_threads_running` instead of `status.global.threads_running`.
It reports all [tags]({{< ref "/config/config-file#tags" >}}) as Prometheus labels.

This sink also accepts the [retry]({{< ref "retry" >}}) options `buffer-size`, `send-timeout`, and `send-retry-wait`.

## Quick Reference

```yaml
sinks:
  chronosphere:
    debug: "no"
    strict-tr: "no"
    url: "http://127.0.0.1:3030/openmetrics/write"
```

## Options

### `debug`

| | |
|-|-|
|**Valid values**|`yes` or `no`|
|**Default value**|`no`|

Print debug output to STDERR.

### `strict-tr`

| | |
|-|-|
|**Valid values**|`yes` or `no`|
|**Default value**|`no`|

Error if metrics cannot be translated to Prometheus/Exposition format.

### `url`

| | |
|-|-|
|**Valid values**|URL|
|**Default value**|`http://127.0.0.1:3030/openmetrics/write`|

URL of Chronocollector.
