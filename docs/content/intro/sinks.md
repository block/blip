---
weight: 5
title: "5. Sinks"
---

# Sinks

<div class="note">
<b>NOTE</b>:
You can skip this part of the introduction if:<br>
<ul>
<li>You use <a href="https://docs.signalfx.com/en/latest/">SignalFx</a>, or</li>
<li>You use <a href="https://www.datadoghq.com/">Datadog</a>, or</li>
<li>You use <a href="https://chronosphere.io/">Chronosphere</a>, or</li>
<li>You plan to use Blip to emulate and replace <a href="https://github.com/prometheus/mysqld_exporter">Prometheus mysqld_exporter</a></li>
</ul>
Blip has built-in support for these use cases.
</div>

Blip ships with built-in support for many common use cases, but the one thing Blip cannot know is where you send metrics.
Consequently, you might need to develop a Blip metric sink to translate and send Blip metrics to your metrics store or metrics graphing solution.
Don't worry: Blip was designed to make this easy.
This brief introduction shows the high-level process of developing a new Blip metric sink.

The following presumes that you're an experienced [Go programmer](https://go.dev/).

All Blip sinks implement this interface:

```go
type Sink interface {
    Send(context.Context, *Metrics) error
    Status() string
}
```

Unsurprisingly, Blip calls the `Send` method to send metrics.
The vast majority of work to implement a new sink is this one method.
More on this in a moment.

Blip calls the `Status` method to report real-time status of the sink (along with all other parts in the monitor).
The reported status can be anything you think is useful to know; for example, the last error sending metrics (if any).

Let's presume, for a moment, that you have implement a new sink.
To allow Blip to make (instantiate) that sink, you implement one last interface:

```go
type SinkFactory interface {
    Make(name, monitorId string, opts, tags map[string]string) (Sink, error)
}
```

When a monitor uses your sink, Blip calls your sink factory to make a new sink for the monitor.
Blip passes to your factory:

* Sink name (which is slightly redundant, but nobody is perfect)
* Monitor ID (all monitors have a unique ID for status, logging, and so forth")
* Options (from the monitor config)
* Tags (from the monitor config)

Options are sink-specific options; for example, an API token is common for authenticating to hosted metrics solutions.
Tags describe the metrics; some metrics solutions calls these "dimensions", others call them "labels"&mdash;Blip calls them "tags".
Options are tags are set in the monitor config, which you'll learn more about later.

Here's a _mock_ (incomplete and nonfunctional) implementation of a sink (called "Kim" for an engineer who left us [the Blip developers] to join a metrics solution startup) and sink factory, just to give you an idea:

```go
import (
    "context"
    "github.com/cashapp/blip/v2"
)

type Kim struct {
    tags   map[string]string
    client Client // sink-specific client
}

func NewKim(monitorId string, opts, tags map[string]string) *Kim {
    return &Kim{
        tags:   tags,
        client: NewClient(opts["addr"], opts["api-token"]),
    }
}

func (k *Kim) Send(cxt context.Context, metrics *blip.Metrics) error {
    // Metrics are grouped by/keyed on domain name
    for domain := range metrics {

        // Loop through metrics in each domain
        for i := range metrics[domain] {

            m := metrics[domain][i]
            // Name:  m.Name  (string)
            // Value: m.Value (float64)
            // Type:  m.Type  (const byte)

            // Transform Blip metrics to sink-specific struct/protocol
        }
    }

    // Send sink-specific struct/protocol (km)
    return k.client.Send(ctx, km)
}

func (k *Kim) Status() string {
    return "I miss where I used to work"
}

// --------------------------------------------------------------------------

type KimFactory struct{}

func (f KimFactory) Make(name, monitorId string, opts, tags map[string]string) (blip.Sink, error) {
    k := NewKim(monitorId, opts, tags)
    return k, nil
}
```

For real sinks, see the built-in Blip sinks: [blip/sinks](https://github.com/cashapp/blip/tree/main/sink).

Once your implementation is done, you register the sink with Blip:

```go
sink.Register("kim", KimFactory{})
```

More on this later; for now, the point is that you register your sink with a given name ("kim"), and that name is important because it's what you specify in a monitor config to make Blip instantiate the sink.
Following is a snippet of a monitor config that shows how the "kim" sink is used and configured:

```yaml
monitors:
  - id: host1
    hostname: host1.local
    sinks:
      kim:
        addr: https://local.domain
        api-token: ABC123
    tags:
      env: staging
      region: us-east-1
```

On line 5, the "kim" sink is specified, and lines 6 and 7 are its options.
Lines 9 and 10 are tags for the monitor, which are also passed to the sink when created.

Bottom line: Blip sinks are pure plugins, so you can make Blip send metrics _anywhere_.

---

Enough talk; let's run: [Quick Start&nbsp;&darr;]({{< ref "quick-start" >}})
