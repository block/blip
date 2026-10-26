---
---

Monitor endpoints return information about monitors and provide some monitor controls through `POST` methods.

An endpoint might reference all monitors or a single monitor as documented.
Single-monitor endpoints require query key `id` to identify the monitor, which is a [`config.monitor.id`]({{< ref "/config/config-file#id" >}}) value.
Use [`GET /monitors`](#get-monitors) to discover monitor IDs if you don't set them explicitly in the Blip config file.

{{< toc >}}

## GET /monitors

Returns a list of all loaded monitors keyed on [`config.monitor.id`]({{< ref "/config/config-file#id" >}}).
The value for each is its redacted DSN (no password).

### Response

```json
{
  "localhost": "blip:...@unix(/tmp/mysql.sock)/?parseTime=true"
}
```

## POST /monitors/reload

Reloads all monitors.
See [Monitors / Loading / Reloading]({{< ref "/monitors/loading#reloading" >}}).

Reloading only affects new and removed monitors.
Monitors that did not change are not affected, even on error.

### Response

None on success (200 status code).

Error message on 4xx or 5xx status code.

### Status Codes

<strong>409</strong>: Error reloading monitors

<strong>412</strong>: [Stop-loss]({{< ref "/monitors/loading#stop-loss" >}}) prevented reloading

## POST /monitors/start?id=ID

Starts one monitor.
`id` query key is required.

If the monitor is still stopping, the request returns 202, and the monitor starts when it has stopped.

### Query

|Key|Value|Required|Purpose|
|---|-----|--------|-------|
|`id`|[`config.monitor.id`]({{< ref "/config/config-file#id" >}})|Yes|Monitor to start|

### Response

None on success (200 status code).

Error message on 4xx or 5xx status code.

### Status Codes

<strong>202</strong>: Monitor is still stopping; it starts when it has stopped

<strong>409</strong>: Error starting monitor, monitor already running or waiting to start, or the `StartMonitor` plugin declined to start it

## POST /monitors/stop?id=ID

Stops one monitor.
`id` query key is required.

The monitor is stopped but not unloaded, which means it reported by status endpoints.

The request waits for the monitor to stop, but no longer than [`config.monitor-loader.stop-timeout`]({{< ref "/config/config-file#stop-timeout" >}}).

### Query

|Key|Value|Required|Purpose|
|---|-----|--------|-------|
|`id`|[`config.monitor.id`]({{< ref "/config/config-file#id" >}})|Yes|Monitor to stop|

### Response

None on success (200 status code).

Error message on 4xx or 5xx status code.

### Status Codes

<strong>202</strong>: Monitor is still stopping after the stop timeout; it keeps stopping in the background
