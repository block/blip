---
---

Implement two interfaces:

* [`blip.Sink`](https://pkg.go.dev/github.com/cashapp/blip/v2#Sink)
* [`blip.SinkFactory`](https://pkg.go.dev/github.com/cashapp/blip/v2#SinkFactory)

Register the custom sink by calling [`sink.Register`](https://pkg.go.dev/github.com/cashapp/blip/v2/sink#Register) before `Server.Boot`.

Reference the custom sink in the [`sinks`]({{< ref "/config/config-file#sinks" >}}) config section:

```yaml
sinks:
  custom-sink:
    opt1: val1
```
