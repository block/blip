---
title: "File"
---

Plans can be written YAML files, deployed with Blip, and used by specifying [`config.plans.files`]({{< ref "/config/config-file#files" >}}).
File paths are relative to the current working directory of `blip`.

{{< toc >}}

## Name

Plan names are _exactly_ as written in [`config.plans.files`]({{< ref "/config/config-file#files" >}}).
For example:

```yaml
plans:
  files:
    - "/blip/plan1.yaml"
    - /plan2.yaml
    - ${HOME}/plan3.yaml
```

The first plan name is `/blip/plan1.yaml` (Blip does not strip base paths).
The second plan name is `/plan2.yaml` (Blip does not replace relative paths).
The third plan name is `/home/user/plan3.yaml` where `HOME=/home/user` because [interpolation](#interpolation) happens first.

## Syntax

A plan file consists of one or more levels with a unique name and frequency.
Each level has the following structure:

```
level-name:
  freq: <duration string>
  collect:
    domain-name:
      <domain config>
```

`level-name` is any name you want to call the level.
`freq` is how often the level is collected, expressed as a [Go duration string](https://pkg.go.dev/time#ParseDuration) like "5s" for "every 5 seconds".
Both level name and frequency must be unique within the same plan (they can be reused in different plans).

Each level has a `collect` subsection under which [domains]({{< ref "/metrics/domains" >}}) are specified.
And each domain has a domain-specific configuration that includes:
* `metrics`: A list of domain-specific metrics to collect
* `options`: A key-value map of options
* `errors`: A key-value map of [error policies]({{< ref "error-policy" >}})

These values are documented for each [domain]({{< ref "/metrics/domains" >}}) and printed on the command line by [`--print-domains`]({{< ref "/config/blip#--print-domains" >}}).

All domains in a plan, across every level, must support at least one common
database type. A database-neutral domain can share a plan with any
database-specific domain, but domains for two different database types cannot
share one plan. A shared plan configuration can still contain separate plans
for different database types.

Since Blip automatically levels up overlapping frequencies (described in [Intro / Plans]({{< ref "intro/plans" >}})), it's conventional to define levels from most to least frequent, as in this example:

```yaml
performance:
  freq: 5s
  collect:
    status.global:
      options:
        all: yes
      metrics:
        # ALL because of option ^
    innodb:
      metrics:
        - trx_rseg_history_len # HLL

data-size:
  freq: 60m
  collect:
    size.database:
      # All defaults
```

The example above specifies two levels: `performance` and `data-size`.

The performance level is collected every 5 seconds&mdash;the most frequent.
For this level, Blip collect metrics from two domains: [`status.global`]({{< ref "domains#statusglobal" >}}) and [`innodb`]({{< ref "domains#innodb" >}}).

The data-size level is collected every 60 minutes&mdash;the least frequent.
For this level, Blip collects metrics for one domain: [`size.database`]({{< ref "domains#sizedatabase" >}}).

When the two levels overlap (every 60 minutes), Blip collects metrics for both levels.
Therefore, you should never repeat metrics (in the same domain) in a plan.
However, you can repeat domains at different levels (and frequencies) like:

```yaml
performance:
  freq: 5s
  collect:
    status.global:
      metrics:
        - Queries
        - Threads_running

standard:
  freq: 20s
  collect:
    status.global:
      metrics:
        - Bytes_sent
        - Bytes_received
```

Every 5 seconds, Blip will collect [`status.global`]({{< ref "/metrics/domains#statusglobal" >}}) metrics `Queries` and `Threads_running`.
And every 20 seconds it will collect those two plus [`status.global`]({{< ref "/metrics/domains#statusglobal" >}}) metrics `Bytes_sent` and `Bytes_received`.

{{< hint type=tip >}}
Repeat domains, never metrics.
{{< /hint >}}

You can repeat domains at different levels to collect more metrics, but don't repeat metrics in a plan.
See also [Metrics / Collecting / Reusing]({{< ref "/metrics/collecting#reusing" >}}).

## Interpolation

Blip interpolates domain option _values_, like:

```yaml
level:
  freq: 10s
  collect:
    domain:
      options:
        foo: "${FOO}"
```

The domain option value `${FOO}` will be replaced with the value of the `FOO` environment variable.

Plan file interpolation has the same syntax and rules as [config file interpolation]({{< ref "/config/interpolation" >}}).
