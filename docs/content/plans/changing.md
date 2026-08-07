---
title: "Changing"
---

Plan changing makes Blip change plans while running (without restarting) based on the state of MySQL:

{{< hint type=note >}}
Plan changing is MySQL-specific. External database monitors do not inherit
global plan-changing defaults and reject monitor-level plan-changing configuration.
{{< /hint >}}

|State|Connected to MySQL|Collecting Metrics|Description|
|-----|------------------|------------------|-----------|
|`offline`|no|no|Completely offline, no connection to MySQL|
|`standby`|**YES**|**YES**|Connected to MySQL but HA passive mode|
|`read-only`|**YES**|**YES**|MySQL is read-only|
|`active`|**YES**|**YES**|MySQL is writable|

Collecting different metrics for different states makes metrics collection more efficient by avoiding unnecessary metrics.
For example, if your environment uses many read replicas, you might not need to collect and report all metrics from read replicas.
However, if those read replicas can be promoted to the active (writable) source on failover, then you still need to collect all metrics if and when that occurs.
This could be solved by reconfiguring and restarting Blip on failover, but it can be done automatically by enabling plan changing.

{{< hint type=note >}}
The `standby` state is not currently used.
It's a placeholder for a feature not yet implemented.
{{< /hint >}}

## Enable

To enable plan changing, configure at least one state in [`config.plans.change`]({{< ref "/config/config-file#change" >}}).

When enabled, states without an explicit plan use [plan precedence]({{< ref "/plans/loading#precedence" >}}) to choose a plan.
However, it is better to explicitly configure all states so it's clear in the config which state uses which plan.

Since plan precedence is scoped, the plan changing configuration can reference plans only in one or the other: monitor plans, or shared plans.

## Disable

Plan changing is entirely disabled when [`config.plans.change`]({{< ref "/config/config-file#change" >}}) is not set.
When disabled, the plan changing code does not run, which means zero additional overhead.
