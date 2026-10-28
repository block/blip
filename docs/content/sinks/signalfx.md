---
title: signalfx
---

{{< hint type=important >}}
Blip works with Splunk, but Splunk does not support or contribute to Blip.
{{< /hint >}}

The signalfx sink sends metrics to [Splunk](https://www.splunk.com/), which [acquired SignalFx](https://www.splunk.com/en_us/newsroom/press-releases/2019/splunk-to-acquire-cloud-monitoring-leader-signalfx.html).

It reports all [tags]({{< ref "/config/config-file#tags" >}}) as dimensions.

This sink also accepts the [retry]({{< ref "retry" >}}) options `buffer-size`, `send-timeout`, and `send-retry-wait`.

## Quick Reference

```yaml
sinks:
  signalfx:
    auth-token: ""
    auth-token-file: ""
    metric-prefix: ""
    metric-translator: ""
```

## Options

### `auth-token`

| | |
|-|-|
|**Valid values**|SignalFx auth token|
|**Default value**||

SignalFx auth token.
Either `auth-token` or `auth-token-file` is required.

### `auth-token-file`

| | |
|-|-|
|**Valid values**|File name|
|**Default value**||

File containing SignalFx auth token.
Either `auth-token` or `auth-token-file` is required.

### `metric-prefix`

| | |
|-|-|
|**Valid values**|String|
|**Default value**||

A string prepended to every metric name before sending.
For example, `metric-prefix: "mysql."` adds "mysql." to the beginning of every metric name.
The string value is literal; Blip does _not_ add a trailing dot.

### `metric-translator`

| | |
|-|-|
|**Valid values**|Registered translator name|
|**Default value**||

Pass metrics through registered metrics translator.
This occurs before `metric-prefix`.
