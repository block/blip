---
---

Collectors are low-level components that collect metrics for [domains]({{< ref "/metrics/domains" >}}).
See [metrics/status.global/global.go](https://github.com/cashapp/blip/blob/main/metrics/status.global/global.go) for a reference example with extensive code comments.

{{< toc >}}

## Domain Names

Blip metric domain names have three requirements:

1. Always lowercase
1. One word: `[a-z]+`
1. Singular noun: "size" not "sizes"; "query" not "queries"

Common abbreviation and acronyms are preferred, especially when they match MySQL usage: "thd" not "thread"; "pfs" not "performanceschema"; and so on.

{{< hint type=note >}}
Currently, domain names fit this convention, but if a need arises to allow hyphenation ("domain-name"), it might be allowed.
Snake case ("domain_name") and camel case ("domainName") are not allowed: the former is used by metrics, and the latter is not Blip style.
{{< /hint >}}

## Subdomains

Blip domains are dot-separated into subdomains, like [`status.global`]({{< ref "/metrics/domains#statusglobal" >}}): domain is `status`, subdomain is `global`.
Blip uses subdomains for [metric grouping]({{< ref "/metrics/reporting#groups" >}}) and, in rare cases, Blip organization.
See the [Domain Quick Reference]({{< ref "/metrics/quick-ref" >}}) for a full list of domains and subdomains.

### Metric Grouping

Many MySQL metrics are grouped, and Blip mirrors the same grouping in its domain names.
This has changed over many years, so if it seems new to you, then consider this:

* Long, log ago only `SHOW STATUS` existed
* Then `SHOW GLOBAL STATUS` came into being
* And now we [Status Variable Summary Tables](https://dev.mysql.com/doc/refman/8.0/en/performance-schema-status-variable-summary-tables.html)

This is why Blip uses domain [`status.global`]({{< ref "/metrics/domains#statusglobal" >}}) and not just "status" because the latter is ambiguous.

{{< hint type=note >}}
For brevity, all Blip domains and subdomains are called "domains".
`repl` is a domain, and `status.global` is a domain.
The subdomain distinction is made only when discussing and naming them.
{{< /hint >}}

Global groups, like [`status.global`]({{< ref "/metrics/domains#statusglobal" >}}) and [`var.global`]({{< ref "/metrics/domains#varglobal" >}}), do _not_ have a [metric group]({{< ref "/metrics/reporting#groups" >}}) because what would the group key and value be?
During Blip developed, we considered a global group key-value like `all="*"` or `all=""`, but these are useless magical values that serve no purpose, so we omitted them.
Groups are only used when there are meaningful non-global group keys and values, like [`size.table`]({{< ref "/metrics/domains#sizetable" >}}): these metrics _must_ be grouped by `db` and `tbl` to make sense.

### Blip Organization

In rare cases, we subdomain for greater clarity and organization in Blip, especially with respect to level collection.
For example, with [`repl`]({{< ref "/metrics/domains#repl" >}}) and [`repl.lag`]({{< ref "/metrics/domains#repllag" >}}) a user might want to collect `repl.lag` frequently (every second) but collect `repl` info more slowly.
If these two were one domain, it would be less clear in the plan and more difficult to code in the collector:

```yaml
rep_lag:
  freq: 1s
  collect:
    repl:
      options:
        source-role: "east"
      metrics:
        - lag

repl_info:
  freq: 30s
  collect:
    repl:
      metrics:
        - running
```

That plan is valid, it's more difficult to code in a single collector given totally different work (and options) at different levels.
While collectors must handle different _metrics_ at different levels, it's usually the same work, which makes coding the collector easier.
Also, the plan is less clear since it's the same domain but configured differently.

By contrast, separate domains makes it easier to develop, test, explain, and use each:

* [`repl`]({{< ref "/metrics/domains#repl" >}}) collects metrics from `SHOW REPLICA STATUS`
* [`repl.lag`]({{< ref "/metrics/domains#repllag" >}}) measures and collects replication lag from [Blip heartbeat]({{< ref "config/heartbeat" >}})

We subdomain for Blip organization judiciously.
If you're making a custom collector that you want to merge upstream (into public Blip), be sure to [file an issue](https://github.com/cashapp/blip/issues) and discuss with us first.

## Metric Names

### MySQL Metrics

Blip reports MySQL metric names as-is (no renaming) so that what you see in MySQL is what you get in Blip.
The only modification Blip makes is lowercasing MySQL metric names for consistency because in MySQL they're inconsistent:

* `Foo_bar` (most common)
* `Foo_Bar` (replica status)
* `foo_bar` (InnoDB metrics)

If your collector only reports MySQL metrics, then just [`strings.ToLower()`](https://pkg.go.dev/strings#ToLower) the name as-is from MySQL.

### Derived Metrics

If your collector creates and reports [derived metrics]({{< ref "/metrics/collecting#derived-metrics" >}}), then there are three requirements for naming:

1. Only `snake_case`
1. Always lowercase
1. No prefixes or suffixes

A fully-qualified metric name includes a domain: `status.global.threads_running`.
The metric name is always the last field (split on `.`).

## Code

Example [https://github.com/cashapp/blip/tree/main/examples/integrate](https://github.com/cashapp/blip/tree/main/examples/integrate) shows how to create a custom metrics collector for domain "foo".
The high-level work is:

1. Implement `blip.Collector` and `blip.CollectorFactory`
2. Register the domain/collector by calling `metrics.Register("foo", myFactory)`
3. Use the domain in a plan:

```yaml
level:
  collect:
    foo: # Custom domain/collector
      metrics:
        - whatever
```

`metrics.Register` preserves the original integration behavior: a factory that
only implements `CollectorFactory` is treated as MySQL. A factory for another
database type implements the optional `CollectorFactoryDatabaseTypes`
capability so Blip can reject an incompatible monitor plan before collector
preparation:

```go
func (myFactory) DatabaseTypes(string) []blip.DatabaseType {
    return []blip.DatabaseType{blip.DatabaseTypePostgres}
}
```

## Long-running

As of Blip v1.2.0, long-running collectors are possible using one of two approaches:

1. Background thread
2. `ErrMore`

But first, it's important to know that when `Collect` is called, it's passed a context (`ctx`) called the _collector max runtime (CMR)_.
The CMR duration is set to the configured level frequency (as set be the user in the plan).
A collector _must_ stop running when the CMR context is cancelled.

### Background Thread

A collector can run background threads (goroutine) to do anything between and during calls to `Collect`.
Then, when `Collect` is called, the collector quickly returns the latest value(s).

Using this method, the collector should return a cleanup function (callback) from `Prepare`.
Blip calls collector cleanup functions if/when a monitor is stopped or restarted.

### ErrMore

If `Collect` returns `ErrMore` (with or without values), Blip keeps calling `Collect` until the collector stops returning `ErrMore` or its CMR expires.
A collector can run for awhile (within its CMR) by returning `ErrMore` to signal that it has more values to report.

When used, the second and subsequent calls to `Collect` have zero value arguments: `nil` context and empty string for level name.
A collector should expect this since it happens only if the collector previously returned `ErrMore`.
