---
weight: 6
title: "Quick Start"
---

Running Blip is the common workflow: _build_ &rarr; _configure_ &rarr; _deploy_.
Blip is a Go program, so building it is trivial.
But like most software, configuring and deploying it will differ based on your particular environment and requirements.
For examples are given that demonstrate common situations.

{{< toc >}}

## Build

To build Blip from source:

```bash
git clone git@github.com:cashapp/blip.git
cd blip
git checkout latest
cd bin/blip
go build
blip --version
```

The binary name is `blip`; this is what you'll deploy alongside your configuration.

## Configure and Deploy

### Developer Laptop

If your local MySQL instance has no root password, then build and run Blip and it should print metrics to STDOUT like:

```
% ./blip
# monitor:  localhost
# plan:     blip
# level:    sysvars
# ts:       2022-10-31T00:00:03.708648-00:00
# duration: 118 ms
status.global.aborted_clients = 595
status.global.aborted_connects = 13
status.global.binlog_cache_disk_use = 5
status.global.bytes_received = 724989491
status.global.bytes_sent = 48547410248
status.global.com_admin_commands = 406
```

If your root user has a password, execute:

```sql
CREATE USER IF NOT EXISTS 'blip' IDENTIFIED BY '';
GRANT SELECT ON *.* TO 'blip'@'%';
```

Then run the `blip` binary.

### Minimum Config and Datadog

This example demonstrates monitoring a single MySQL instance with a minimum Blip configuration and sending metrics to Datadog.
Even if you don't use Datadog, this example demonstrates how to configure a sink because without a sink Blip prints metrics to STDOUT.

{{< hint type=important >}}
Blip metrics are billed by Datadog as custom metrics because they do not original from a Datadog agent.
{{< /hint >}}

For simplicity, let's presume that you're going to monitor one MySQL instance with this DSN:

|Hostname|`db.local`|
|Username|`blip`|
|Password|`blip`|

First create the MySQL user for Blip on `db.local`:

```sql
CREATE USER IF NOT EXISTS 'blip' IDENTIFIED BY 'blip';
GRANT SELECT, REPLICATION CLIENT ON *.* TO 'blip'@'%';
```

{{< hint type=important >}}
Replace the password with something better in the SQL above and YAML below, especially if you're doing this on a real production server.
{{< /hint >}}

Save the following YAML config file as `blip.yaml`:

```yaml
monitors:
  - hostname: db.local
    username: blip
    password: blip

sinks:
  datadog:
    api-key-auth: "..."
    app-key-auth: "..."
```

Replace `...` with real Datadog API credentials.

Deploy the binary `blip` and its config file `blip.yaml` however you deploy services.
Put both files in the  same directory (Blip loads `blip.yaml` in the current working directory by default), and run `blip`.

If successful, you should see MySQL metrics in Datadog.
Look for metrics like `status.global.queries` and `status.global.threads_running`.

### Two MySQL and Custom Plan

This example demonstrate monitoring two MySQL instances with a single Blip instance, and using a custom plan to collect specific metrics.

For simplicity, let's presume that you're going to monitor MySQL instance with these DSNs:

|Hostname|`host-A`|`host-B`|
|Username|`blip`|`blip`|
|Password|`blip`|`blip`|

Save the following YAML config file as `blip.yaml`:

```yaml
mysql:
  username: blip
  password: blip

plans:
  files:
    - kpi.yaml

monitors:
  - hostname: host-A

  - hostname: host-B
```

The monitors inherit values from the top-level `mysql` section, which is why we can configure the username and password once for both instances.
Likewise for the top-level `plans`: both monitors inherit this value since they don't explicitly set/override it the `monitors` section.

Write and save the following plan as `kpi.yaml`:

```yaml
kpi:
  freq: 1s
  collect:
    status.global:
      metrics:
        - Queries
        - Threads_running
```

Deploy all three together in the same directory: `blip` binary, `blip.yaml` config file, and `kpi.yaml` plan file.
Then run `blip` and it should print two metrics every second, for each MySQL instance:

```
# monitor:  host-A
# plan:     kpi.yaml
# level:    kpi
# ts:       2022-10-31T00:00:00.50252-00:00
# duration: 2 ms
status.global.queries = 20850006
status.global.threads_running = 2
```

When plans are configured, Blip uses those&mdash;your plans.
Else, it defaults to its built-in plan, which is a really good starting point, but experienced DBAs will probably want to write a custom plan.

### Custom-built Blip

The example demonstrates how to import Blip so that you can customize and wrap it in your own service container.
Blip has many customizations, but let's implement a common one: a custom sink called "yaml" that dumps metrics to STDOUT in YAML format.

For simplicity, copy-paste the code and build it on your laptop with a local MySQL instance and a `root` or `blip` MySQL user without a password.
(See [Developer Laptop](#developer-laptop) above for this setup.)

```bash
mkdir ~/blip-custom-sink
cd ~/blip-custom-sink
vi main.go
# Copy-paste the code below
go mod init custom-sink
go mod tidy
go build
./custom-sink
```

```go
package main

import (
    "context"
    "fmt"
    "log"

    "gopkg.in/yaml.v2"

    "github.com/cashapp/blip/v2"
    "github.com/cashapp/blip/v2/server"
    "github.com/cashapp/blip/v2/sink"
)

type YAML struct{}

var _ blip.Sink = YAML{}
var _ blip.SinkFactory = YAML{}

func (y YAML) Send(ctx context.Context, m *blip.Metrics) error {
    bytes, err := yaml.Marshal(m)
    if err != nil {
        return err
    }
    fmt.Println(string(bytes))
    return nil
}

func (y YAML) Name() string {
    return "yaml"
}

func (y YAML) Make(_ blip.SinkFactoryArgs) (blip.Sink, error) {
    return YAML{}, nil
}

func main() {
    sink.Register("yaml", YAML{}) // Register custom "yaml" sink
    sink.Default = "yaml"         // Change default sink to ^

    // Create, boot, and run the custom Blip server
    s := server.Server{}
    if err := s.Boot(server.Defaults()); err != nil {
        log.Fatalf("server.Boot failed: %s", err)
    }
    if err := s.Run(server.ControlChans()); err != nil { // blocking
        log.Fatalf("server.Run failed: %s", err)
    }
}
```

If successful, Blip should print a long output of YAML like:

```
begin: 2022-10-30T18:12:41.456114-00:00
end: 2022-10-30T18:12:41.464785-00:00
monitorid: localhost
plan: blip
level: sysvars
state: ""
values:
  innodb:
  - name: lock_timeouts
    value: 0
    type: 1
    group: {}
    meta:
      subsystem: lock
  - name: lock_row_lock_current_waits
    value: 0
    type: 2
    group: {}
    meta:
      subsystem: lock
```

To learn more about custom building Blip, start with section [Develop]({{< ref "/develop" >}}).

## Next Steps

The three most important parts of running Blip are:

1. The [config file]({{< ref "config/config-file" >}}) to configure MySQL instances and metric sinks
2. The [plan file]({{< ref "plans/file" >}}) to collect only the metrics you need
3. [Developing a custom sink]({{< ref "develop/sinks" >}}) if you don't use a [built-in sinks]({{< ref "/sinks" >}})
