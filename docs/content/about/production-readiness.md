---
weight: 2
---

A monitor must be more reliable than what it monitors.
But the paradox is that reliability is only achieved through extensive real-world usage.
Consequently, Blip is released with features at different levels of readiness:

New
: Feature is new and not widely tested in the real world. Expect bugs. Ready for early adopters.

Stable
: Feature seems stable in the real world, but it's still considered new. There might be bugs. Ready for pre-production testing and burn-in.

<span class="ga">Production</span>
: Feature has been running in the real world for several months. Bugs are not expected. Ready for production.

Feature readiness is documented here to help you make informed decisions about monitoring your databases with Blip.

## v2.x

Blip v2.x is Stable. Existing production-ready components retain their readiness; collectors and extension APIs introduced in v2.1 are marked New.

### Metric Collectors

|Domain|Readiness|
|-------|------|
|[autoinc]({{< ref "metrics/domains/autoinc/" >}})|New|
|[aws.rds]({{< ref "metrics/domains/aws.rds/" >}})|<span class="ga">Production</span>|
|[error.account]({{< ref "metrics/domains/error.account/" >}})|New|
|[error.global]({{< ref "metrics/domains/error.global/" >}})|New|
|[error.host]({{< ref "metrics/domains/error.host/" >}})|New|
|[error.thread]({{< ref "metrics/domains/error.thread/" >}})|New|
|[error.user]({{< ref "metrics/domains/error.user/" >}})|New|
|[innodb]({{< ref "metrics/domains/innodb/" >}})|<span class="ga">Production</span>|
|[innodb.buffer-pool]({{< ref "metrics/domains/innodb.buffer-pool/" >}})|New|
|[repl]({{< ref "metrics/domains/repl" >}})|<span class="ga">Production</span>|
|[repl.lag]({{< ref "metrics/domains/repl.lag/" >}})|<span class="ga">Production</span>|
|[size.binlog]({{< ref "metrics/domains/size.binlog/" >}})|<span class="ga">Production</span>|
|[size.database]({{< ref "metrics/domains/size.database/" >}})|<span class="ga">Production</span>|
|[size.table]({{< ref "metrics/domains/size.table/" >}})|<span class="ga">Production</span>|
|[status.global]({{< ref "metrics/domains/status.global/" >}})|<span class="ga">Production</span>|
|[stmt.current]({{< ref "metrics/domains/stmt.current/" >}})|<span class="ga">Production</span>|
|[tls]({{< ref "metrics/domains/tls/" >}})|New|
|[var.global]({{< ref "metrics/domains/var.global/" >}})|<span class="ga">Production</span>|
|[wait.io.table]({{< ref "metrics/domains/wait.io.table/" >}})|Stable|

### Sinks

|Sink|Readiness|
|-------|------|
|[chronosphere]({{< ref "sinks/chronosphere" >}})|New|
|[datadog]({{< ref "sinks/datadog" >}})|<span class="ga">Production</span>|
|[log]({{< ref "sinks/log" >}})|<span class="ga">Production</span>|
|[prom-pushgateway]({{< ref "sinks/prom-pushgateway" >}})|New|
|[retry]({{< ref "sinks/retry" >}})|Stable|
|[signalfx]({{< ref "sinks/signalfx" >}})|<span class="ga">Production</span>|

### Cloud

|Feature|Readiness|
|-------|------|
|[AWS IAM auth]({{< ref "cloud/aws" >}})|Stable|
|[AWS Secrets Manager]({{< ref "cloud/aws" >}})|New|

### General

|Feature|Readiness|
|-------|------|
|[API]({{< ref "api" >}})|Stable|
|[External database modules]({{< ref "develop/database-modules" >}})|New|
|[Heartbeat]({{< ref "config/heartbeat" >}})|Stable|
|[Monitor Loading Stop-loss]({{< ref "monitors/loading#stop-loss" >}})|New|
|[Plan Changing]({{< ref "plans/changing" >}})|New|
|[Plan Error Policy]({{< ref "plans/error-policy" >}})|Stable|
|[Plan File]({{< ref "plans/file" >}})|<span class="ga">Production</span>|
|[Plan Table]({{< ref "plans/table" >}})|New|
|[Prometheus emulation]({{< ref "prometheus" >}})|New|
