# Blip Example: Copying Metric Metadata

This example demonstrates a `TransformMetrics` plugin that copies metric metadata from one domain to another.

Presume the replication topology is:

```
01 --> 02 --> 03
```

Node 01 is the source on which heartbeats are written.
Node 02 is an intermediate replica.
Node 03 is a replica of 02.

**We are monitoring node 03.**

The plan is:

```yaml
level:
  collect:
    repl:
      metrics:
        - running
    repl.lag:
      # Defaults
```

Before transformation, metrics will be:

```js
{
  repl: {
    {
      Name: "running"
      Value: 1
      Meta: {
        "source": "02"  // immediate source
      }
    }
  }
  repl.lag: {
    {
      Name: "current"
      Value: 75
      Meta: {
        "source": "01"  // heartbeat source
      }
    }
  }
}
```

The `source` meta value for each domain is different:

* `repl` reports the _immediate source_: the configured source hostname as shown by `SHOW REPLICA STATUS`
* `repl.lag` reports the _heartbeat source_: the source ID that wrote the heartbeat

These two are difference because of the multi-level replication topology: 02 is the immediate source of 03, but 01 is the heartbeat source.

**This plugin sets `repl.lag` `source` meta value to the immediate source: 02.**
