---
title: "repl.lag"
---

The `repl.lag` domain includes metrics from multiple sources related to replication lag and event processing.

{{< hint type=note >}}
This domain does _not_ collect `Seconds_Behind_Source` (fka `Seconds_Behind_Master`) because this historical metric is not an industry best practice.
Instead, use Blip heartbeats or Performance Schema.
{{< /hint >}}

{{< toc >}}

## Usage

There are two replication lag writers:

|&nbsp;|Blip Heartbeat|MySQL 8.x Performance Schema|
|---|---|---|
|**Preferred**|No|Yes, [`writer = auto`](#writer)|
|**External Setup**|Yes|No|
|**Extra User Privs**|Yes|No|
|**MSR and MTR**|No|Yes|
|**MySQL Version**|Any|8.x|

If running MySQL 8.x, use the Performance Schema.

The [Blip heartbeat]({{< ref "config/heartbeat" >}}) is the legacy writer and should be used only when needed.

The main derived metric is `current` that reports current replication lag in milliseconds.
On MySQL 8.x, Performance Schema is used to report other derived metrics.


When using MySQL 8.x Performance Schema, metrics are [grouped](#group-keys) by channel name.

## Derived Metrics

### `backlog`

| | |
|---|---|
|**Metric Type**|gauge|
|**Value Units**|events [0, inf.)|
|[**Writer**](#writer)|`pfs`|

Number of events not applied.

Only available with MySQL 8.x Performance Schema.

### `current`

| | |
|---|---|
|**Metric Type**|gauge|
|**Value Units**|milliseconds|
|[**Writer**](#writer)|Any|

The current replication lag in milliseconds.

### `worker_usage`

| | |
|---|---|
|**Metric Type**|gauge|
|**Value Units**|percentage [0, 1.0]|
|[**Writer**](#writer)|`pfs`|

Percentage (0.0-1.0) of applier threads (worker) seen applying during collection.

Only available with MySQL 8.x Performance Schema.

## Options

When `repl.lag` is collected at more than one level, `writer`, `repl-check`, `table`, `source-id`, `source-role`, and `network-latency` are read from one level only: the first level, by name, that sets `writer` to `pfs` or `blip`, else the first level that collects `repl.lag`.
The options `report-no-heartbeat`, `report-not-a-replica`, and `default-channel-name` apply per level.
If one level sets `writer: pfs` and another sets `writer: blip`, the plan fails to prepare.

### Common

#### `repl-check`

| | |
|---|---|
|**Value**|MySQL global variable (without @@)|
|**Default**||

If the given MySQL global variable equal zero, the instance is _not_ a replica.
Any other value and the instance is considered a replica.

#### `writer`

|Value|Default|Description|
|---|---|---|
|auto |&check;|Use `pfs` if available, else use `blip`|
|blip| |Use [Blip heartbeat]({{< ref "config/heartbeat/" >}})|
|pfs | |Use MySQL 8.x Performance Schemna tables|

What is writing replication heartbeats or events.

### MySQL 8.x Performance Schmea

#### `default-channel-name`

| | |
|---|---|
|**Value Type**|string|
|**Default**||

Set to rename default channel name from an empty string (the MySQL default) to a non-empty string.
Metrics are [grouped](#group-keys) by channel name.

### Blip Heartbaet

#### `network-latency`

| | |
|---|---|
|**Value Type**|[Duration string](https://pkg.go.dev/time#ParseDuration)|250ms|
|**Default**|50ms|

How long to wait for the next Blip heartbeat in addition to its planned arrival time.
This amount is also subtracted from the `current` value.

For example, if the next heartbeat is scheduled to arrive at 12:00:00.000 and network latency is 50ms, Blip waits until 12:00:00.050 to read the heartbeat.

#### `report-no-heartbeat`

Value|Default|Description|
|---|---|---|
|yes||Report `current = -1` if no heartbeat|
|no|&check;|Drop `current` metric if no heartbeat|

#### `report-not-a-replica`

Value|Default|Description|
|---|---|---|
|yes||Report `current = -1` if not a replica|
|no|&check;|Drop `current` metric if not a replica|

#### `source-id`

| | |
|---|---|
|**Value**|string|
|**Default**||

See [Config / Heartbeat]({{< ref "config/heartbeat/#replication-topology" >}}) for details.

#### `source-role`

| | |
|---|---|
|**Value**|string|
|**Default**||

See [Config / Heartbeat]({{< ref "config/heartbeat/#replication-topology" >}}) for details.

#### `table`

| | |
|---|---|
|**Value**|string|
|**Default**|`blip.heartbeat`|

See [Config / Heartbeat -- Table]({{< ref "config/heartbeat/#table" >}}) for details.

## Group Keys

Only when using MySQL 8.x Performance Schema:

|Key|Value|
|---|---|
|`channel_name`|`CHANNEL_NAME` column value|

Like MySQL, the default channel name is an empty string.
For Blip reporting, this can be changed with option [`default-channel-name`](#default-channel-name).

## Meta

None.

## Error Policies

None.

## MySQL Config

MySQL must be configured as a replica, and the Performance Schema must be enabled.

## Changelog

|Blip Version|Change|
|------------|------|
|v1.1.0      |&bull; Added support for MySQL 8.x Performance Schema<br>&bull; Default [`writer`](#writer) changed from "blip" to "auto", preferring Performance Schema ("pfs")|
|v1.0.0      |Domain added|
