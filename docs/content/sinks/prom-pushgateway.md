---
title: "prom-pushgateway"
---

{{< hint type=warning title=Experimental >}}
The prom-pushgateway sink is new as of Blip v1.1.0 and **experimental**.
Use with caution.
{{< /hint >}}

The prom-pushgateway sink makes Blip act like a [Prometheus Pushgateway](https://github.com/prometheus/pushgateway).
It uses Prometheus domain translation, so it only works with [compatible domains]({{< ref "config/prometheus/#compatibile-domains" >}}).

Currently, this sink is intended to work with [Vector by Datadog](https://vector.dev/).
Therefore, it uses only `POST` and the text (exposition) protocol; it does not use RPC with protos.

This sink also accepts the [retry]({{< ref "retry" >}}) options `buffer-size`, `send-timeout`, and `send-retry-wait`.

## Quick Reference

```yaml
sinks:
  prom-pushgateway:
    addr: "http://127.0.0.1:9091"
```

## Options

### `addr`

| | |
|-|-|
|**Valid values**|URL|
|**Default value**|http://127.0.0.1:9091|

URL to push metrics to.
"http://" prefix required.
