---
title: "tls"
---

The `tls` domain includes metrics about the status and configuration of TLS (SSL).

{{< toc >}}

## Usage

Industry best practice is to always use TLS with MySQL.
This domain reports a single derived metric, `enabled`, that should be monitored to ensure that every MySQL instance has TLS enabled.

## Derived Metrics

### `enabled`

| | |
|---|---|
|**Metric Type**|bool|
|**Value Units**||

True (1) if the main MySQL connection interface supports encrypted connections, else false (0).
Metrics sinks that don't support bool report this metric as a gauge.

{{< hint type=note >}}
On MySQL 8.0.21 and newer, Blip reads the `mysql_main` channel's `Enabled` property from the [`tls_channel_status` table](https://dev.mysql.com/doc/refman/8.0/en/performance-schema-tls-channel-status-table.html).
For older MySQL versions and compatible distributions that do not have this table, or when the table cannot be read, Blip falls back to `have_ssl` when that variable is available.
{{< /hint >}}

## Options

None.

## Group Keys

None.

## Meta

None.

## Error Policies

None.

## MySQL Config

On servers without the legacy `have_ssl` variable, including MySQL 8.4 and newer, the Blip database user needs `SELECT` access to `performance_schema.tls_channel_status`.
Earlier versions can use the legacy `have_ssl` fallback when this table is unavailable or inaccessible.

## Changelog

|Blip Version|Change|
|------------|------|
|v1.0.0      |Domain added|
