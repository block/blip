---
title: retry
---

The retry sink is a pseudo-sink that provides buffering, serialization, and retry for real sinks.
The built-in sinks, except [`log`]({{< ref "log" >}}) and `noop`, use a retry sink to handle those three complexities.

The retry sink uses a LIFO queue (a stack) to prioritize sending the latest metrics.
During a long outage of the real sink, the retry sink drops the oldest metrics and keeps the latest metrics, up to its buffer size, which is configurable.

Retry is not configured as a separate sink: a `sinks.retry` section fails with "sink retry not registered".
Set the retry options in the options of each built-in sink that uses it, alongside that sink's own options.
An invalid value prevents the monitor from loading.

## Quick Reference

```yaml
sinks:
  datadog:
    # datadog options
    buffer-size: 60
    send-timeout: 5s
    send-retry-wait: 200ms
```

## Options

### `buffer-size`

| | |
|-|-|
|**Valid values**|Integer from 1 to 10000|
|**Default value**|60|

Maximum number of metrics batches buffered while the real sink is failing.
When the buffer is full, the oldest batch is dropped.

### `send-timeout`

| | |
|-|-|
|**Valid values**|[Go duration string](https://pkg.go.dev/time#ParseDuration) greater than zero and at most 1m|
|**Default value**|5s|

Maximum time for each send cycle, in which buffered metrics batches are sent to the real sink, newest first.
When the timeout expires, the send in progress is cancelled and the cycle ends; a real sink that ignores cancellation can run past it.
Batches not sent in time stay in the buffer for the next cycle.
A monitor finishes each send cycle before it sends metrics to its next sink, so a long timeout delays the monitor's other sinks during an outage.

### `send-retry-wait`

| | |
|-|-|
|**Valid values**|[Go duration string](https://pkg.go.dev/time#ParseDuration) greater than zero|
|**Default value**|200ms|

Wait time between sends to the real sink within one send cycle.
The wait ends early when `send-timeout` expires, which ends the cycle.
