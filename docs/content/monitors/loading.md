---
title: "Loading"
---

The simplest way to load monitors is by specifying [`config.monitors`]({{< ref "config/config-file#monitors" >}}):

```yaml
monitors:
  - hostname: db1.local
    username: blip
    password: ...

  - hostname: db2.remote
    username: blip
    password: ...
```

Blip loads the two monitors specified in the config file.
This is the simple case, but Blip can load monitors from other sources that support reloading.

## Sources

The load squence is:

1. [`LoadMonitors` plugin]({{< ref "/develop/integration-api#plugins" >}}) _exclusively_ if defined; else:
2. [`config.monitors`]({{< ref "/config/config-file#monitors" >}}), and then
3. [`config.monitor-loader.files`]({{< ref "/config/config-file#files" >}}), and then
4. [`config.monitor-loader.aws`]({{< ref "/config/config-file#aws" >}}); if no monitors loaded, then
5. Auto-detect local MySQL instances

### Reloading

Calling API endpoint [`/monitors/reload`]({{< ref "/api/monitors#post-monitorsreload" >}}) causes Blip to reload monitors for supported sources:

|Source|Reloads?|
|------|---------|
|[`LoadMonitors` plugin]({{< ref "/develop/integration-api#plugins" >}})|Yes|
|[`config.monitors`]({{< ref "/config/config-file#monitors" >}})|No|
|[`config.monitor-loader.files`]({{< ref "/config/config-file#files" >}})|Yes|
|[`config.monitor-loader.aws`]({{< ref "/config/config-file#aws" >}})|Yes|
|Auto-detect local|No|

New monitors are started.
Monitors that have been removed (no longer returned by the source) are unloaded (stopped and removed) from Blip.
Monitors that have not change are not affected or restarted.

Removed monitors are stopped in parallel.
A reload waits for them to stop, but no longer than [`config.monitor-loader.stop-timeout`]({{< ref "/config/config-file#stop-timeout" >}}).

### Stop-loss

Stop-loss prevents reloading from dropping too many MySQL instances due to unrelated external issues.
For example, maybe the `LoadMonitors` plugin has a bug that causes it to return zero monitors when there are really 10 monitors.
Stop-loss would prevent Blip from unloading all monitors, which could have adverse side effects for visibility and alerting.

Set [`config.monitor-loader.stop-loss`]({{< ref "/config/config-file#stop-loss" >}}) to enable.
