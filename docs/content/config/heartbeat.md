---
---

Blip heartbeat works in conjunction with the [`repl.lag` metric collector](metrics/domains#repllag) to measure replication lag.
Although MySQL has built-in replication heartbeats and lag metrics, they are not always enabled or accurate.
For example, `Seconds_Behind_Source` from `SHOW REPLICA STATUS` (or `Seconds_Behind_Master` from `SHOW SLAVE STATUS` before MySQL 8.022) is always on but infamously inaccurate: it reports zero when a network issue blocks replication.
Consequently, external replication heartbeats are an industry norm because they are easy and accurate&mdash;and they work the same across all versions and distributions of MySQL, including the the cloud.

{{< hint type=note >}}
Blip heartbeat is MySQL-specific. External database monitors do not inherit
global heartbeat defaults and reject monitor-level heartbeat configuration.
{{< /hint >}}

## Quick Start

Presuming one source MySQL instance and one read-only replica, the minimal configuration is:

1. Create the [heartbeat table](#table) on the source.
2. Grant the Blip MySQL user these privileges:<br/>&bull; [`REPLICATION CLIENT ON *.*`](https://dev.mysql.com/doc/refman/en/privileges-provided.html#priv_replication-client)<br/>&bull; `SELECT, INSERT, UPDATE, DELETE ON blip.heartbeat`
3. Enable the heartbeat in the Blip config:
  ```yaml
heartbeat:
  freq: 2s
````
4. Enable the `repl.lag` metric collector in the Blip plan:
```yaml
level:
  collect:
       repl.lag:
```

With this minimal configuration, Blip tries and fails to write heartbeats on the read-only replica, but it keeps trying in the expectation that the replica can become the source after a failover.

## Table

The default heartbeat table is `blip.heartbeat`:

```sql
CREATE TABLE IF NOT EXISTS heartbeat (
  src_id   varchar(200)      NOT NULL PRIMARY KEY,
  src_role varchar(200)          NULL DEFAULT NULL,
  ts       timestamp(3)      NOT NULL,  -- heartbeat
  freq     smallint unsigned NOT NULL   -- milliseconds
) ENGINE=InnoDB;
```

## Replication Topology

Replication lag is a point-to-point measurement between a source and a replica, but replication topologies change due to maintenance and failures.
That makes it difficult to configure heartbeat because sources and replicas change.
Even though the [plan changing](plans/changing) can change the Blip configuration based on the state of MySQL, that is not sufficient when there are more than three or more  nodes in the replication topology and any node might become the source.

To address these challenges, Blip heartbeat has two concepts: _source reporting_ and _source following_.

### Source Reporting

Source reporting determines the value a monitor reports as its replication source ID (or "source" for short).
By default, a monitor reports `monitor.id` as its source.

The default works if MySQL nodes have valid hostnames _and_ replicas are configured to use those hostnames.
But this is not always the case, especially in the cloud.
To override the default, set `config.heartbeat.source` (or `config.monitors.heartbeat.source`) to report a different source value:

```yaml
monitors:
  - id: host1.local
    heartbeat:
      source: "node1"
```

The config snippet above will make monitor `host1.local` report itself as replication source ID `node1`.
Here's a more advanced configuration that does the same but uses config defaults and interpolation:

```yaml
heartbeat:
  source: "%{monitor.tags.db_id}"

monitors:
  - id: host1.local
    tags:
      db_id: "node1"
```

The monitor must report a replication source ID when heartbeat is enabled, and every source must be unique in the replication topology.
A monitor can also report an optional _source role_: a user-defined value that multiple nodes in the replication can report (a shared value).
For example, suppose that a replication topology has four nodes in two different regions.
The heartbeat config might look like:

```yaml
heartbeat:
  role: "%{monitor.tags.region}"

monitors:
  - id: host1.local
    tags:
      region: "west"

  - id: host2.local
    tags:
      region: "west"

  - id: host3.local
    tags:
      region: "east"

  - id: host4.local
    tags:
      region: "east"
```

Nodes `host1` and `host2` report role `west`, and nodes `host3` and `host4` report role `east`.
Roles are used for replication following.

### Source Following

Source following refers to the method by which a monitor determines the source from which the lag is measured and reported using the [`repl.lag` collector](metrics/domains#repllag).
The three methods in order of precedence for most to least specific: _source ID_, _role_, and _latest_.

{{< hint type=note >}}
Blip heart does not automatically find the source of a replica, but this feature might be added later.
It does not support [MySQL multi-source replication](https://dev.mysql.com/doc/refman/en/replication-multi-source.html), and there are no plans to support this feature.
{{< /hint >}}

**Source ID**

```yaml
level:
  collect:
    repl.lag:
      options:
        source-id: "node1"
```

If the `source-id` option is specified, the monitor will report replication lag only from the monitor reporting as `node1`, in this example.
Option `source-id` takes precedent over other options because it's the most specific.

**Role**

```yaml
level:
  collect:
    repl.lag:
      options:
        source-role: "east"
```

If the `source-role` option is specified, the monitor will report replication lag from the _latest timestamp_ of any monitor reporting role `east`, in this example.
This is useful when a set of nodes replicate only from another specific set of nodes, such as nodes in a disaster recovery (DR) region replicating only from nodes in the primary (or active) region.
In this case, monitors in each region can report a role and follow the role of the other region.

Following is an advanced example of the monitor and plan configuration for two nodes in two regions where the one follows the older based on DR region (using config interpolation):

```yaml
heartbeat:
  role: "%{monitor.tags.region}"

monitors:
  - id: host1.local
    tags:
      region: "west"
      dr_region: "east"

  - id: host3.local
    tags:
      region: "east"
      dr_region: "west"

```

```yaml
level:
  collect:
    repl.lag:
      options:
        source-role: "%{monitor.tags.dr_region}"
```

`host1` is active in region `west` with DR region `east`.
`host2` is the opposite: active in `east` with DR in `west`.
If `host1` is the source, then `host3` will report replication from it because it's configured to follow any monitor reporting its DR region (`west`), which is what `host1` reports as its role.
After DR failover, the situation reverse: `host3` reports its role as `east`, which is what `host1` is configured to follow since that is its DR region.

**Latest**

```yaml
level:
  collect:
    repl.lag:
      # Defaults
```

If `source-id` and `source-role` are not specified (the default), the monitor will report replication lag from the _latest timestamp_ of any monitor.
This is useful when the replication topology guarantees that only one node is writable (`read_only=0`) at all times, which is typical for MySQL replication topologies.
In this case, every monitor follows (reports replication lag) from whichever node happens to be the (writable) source, as indicated by the fact that it's able to write heartbeats.

## Repl Check

The [`repl.lag` collector](metrics/domains#repllag) option `repl-check` is used to ignore (not report) replication lag on nodes that are not actually replicas.
This is common in the cloud: a _reader_ instance is not a replica but, rather, another MySQL instance that reads data from shared (network-backed) storage.
A reader will see heartbeats from the source, but this is not true replication lag.

The `repl-check` value must be a global MySQL system variable ("sysvar").
If the sysvar value is zero, then Blip does not report replication lag.
[`server_id`](https://dev.mysql.com/doc/refman/en/replication-options.html#sysvar_server_id) is the recommended sysvar because reader instances typically have this set to zero.
On failover&mdash;when a reader becomes the writer&mdash;`server_id` is set to a non-zero value, which makes Blip replication reporting follow the reader-writer changes in the cloud.

{{< hint type=note >}}
Repl check does not use `SHOW REPLICA STATUS` because that can, in rare cases, be slow to respond or impact MySQL.
Using a global MySQL system variable is extremely fast and zero impact.
{{< /hint >}}

## Accuracy

Blip heartbeat uses a new approach to replication lag monitoring that decouples read accuracy from write frequency.
Blip can write heartbeats every 2 seconds, read them every 5 seconds, and still measure (and report) sub-second lag.
It is not necessary (or advised) to configure high-frequency heartbeats.
The recommended heartbeat frequency is 2 seconds:

```yaml
heartbeat:
  freq: 2s
```

Blip _always_ measures and reports replication lag with sub-second precision.
(The replication lag metric, `repl.lag.current`, is reported in milliseconds.)
The heartbeat and plan level frequencies do not affect replication lag accuracy.
The former determines how frequently replication lag is tested.
The latter determines how frequently replication lag is reported as a metric.

Like all external replication heartbeats, accuracy is affected by the clock skew between the source and replica.
Blip presumes clock skew is far less than network latency (between source and replica) such that inaccuracy is overwhelmingly due to fluctuations in network latency.
The [`repl.lag` collector](metrics/domains#repllag) option `network-latency` (default: 50 ms) accounts for this presumption.
If the source writes a heartbeat at time `T`, then it should arrive on the replica `T + network-latency`, presuming no appreciable clock skew.
Blip checks for the heartbeat at `T + network-latency` and subtracts `network-latency` from the difference of the heartbeat timestamps.
If clock skew is negligible, and network latency is steady as configured, and MySQL replication is not lagging, then Blip will measure near-zero lag (skewed only by the few microseconds it takes MySQL to execute the lag check query).
