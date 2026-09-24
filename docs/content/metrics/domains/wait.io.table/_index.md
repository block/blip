---
title: "wait.io.table"
---

The `wait.io.table` domain includes summarized table I/O wait metrics from Performance Schema table [`table_io_waits_summary_by_table`](https://dev.mysql.com/doc/mysql-perfschema-excerpt/en/performance-schema-table-io-waits-summary-by-table-table.html).

{{< toc >}}

## Usage

```
mysql> SELECT * FROM performance_schema.table_io_waits_summary_by_table LIMIT 1\G
*************************** 1. row ***************************
     OBJECT_TYPE: TABLE
   OBJECT_SCHEMA: mysql
     OBJECT_NAME: dd_properties
      COUNT_STAR: 0
  SUM_TIMER_WAIT: 0
  MIN_TIMER_WAIT: 0
  AVG_TIMER_WAIT: 0
  MAX_TIMER_WAIT: 0
      COUNT_READ: 0
  SUM_TIMER_READ: 0
  MIN_TIMER_READ: 0
  AVG_TIMER_READ: 0
  MAX_TIMER_READ: 0
     COUNT_WRITE: 0
 SUM_TIMER_WRITE: 0
 MIN_TIMER_WRITE: 0
 AVG_TIMER_WRITE: 0
 MAX_TIMER_WRITE: 0
     COUNT_FETCH: 0
 SUM_TIMER_FETCH: 0
 MIN_TIMER_FETCH: 0
 AVG_TIMER_FETCH: 0
 MAX_TIMER_FETCH: 0
    COUNT_INSERT: 0
SUM_TIMER_INSERT: 0
MIN_TIMER_INSERT: 0
AVG_TIMER_INSERT: 0
MAX_TIMER_INSERT: 0
    COUNT_UPDATE: 0
SUM_TIMER_UPDATE: 0
MIN_TIMER_UPDATE: 0
AVG_TIMER_UPDATE: 0
MAX_TIMER_UPDATE: 0
    COUNT_DELETE: 0
SUM_TIMER_DELETE: 0
MIN_TIMER_DELETE: 0
AVG_TIMER_DELETE: 0
MAX_TIMER_DELETE: 0
```

Each column that does not begin with `OBJECT_` is a metric that can be collected.
For example, to collect only write-related metrics:

```yaml
level:
  collect:
    wait.io.table:
      metrics:
        - COUNT_WRITE
        - SUM_TIMER_WRITE
        - MIN_TIMER_WRITE
        - AVG_TIMER_WRITE
        - MAX_TIMER_WRITE
```

{{< hint type=note >}}
All Blip metric names are lowercase when reported.
{{< /hint >}}

By default, metrics are [grouped](#group-keys) by database and table. Set `group-by: schema` to report one series per database instead.

## Derived Metrics

None.

## Options

### `group-by`

|Value|Default|Description|
|-----|-------|-----------|
|table|&check;|One series per table, grouped by `db` and `tbl`|
|schema||One series per schema, grouped by `db`|
|both||Table and schema series|

Schema rollups sum counts and total wait times across tables. Minimum and maximum times span the tables with events; averages are weighted by the event count. These are table handler waits, not physical disk I/O or complete query latency.

### `all`

|Value|Default|Description|
|-----|-------|-----------|
|yes  | |Collect all columns in the table|
|no   |&check;|Collect only columns listed in the plan|

### `exclude`

| | |
|---|---|
|**Value Type**|CSV string of db.table|
|**Default**|`mysql.*,information_schema.*,performance_schema.*,sys.*`|

A comma-separated list of database or table names to exclude (ignored if `include` is set).

### `include`

| | |
|---|---|
|**Value Type**|CSV string of db.table|
|**Default**||

A comma-separated list of database or table names to include (overrides option `exclude`).

### `truncate-table`

|Value|Default|Description|
|---|---|---|
|yes|&check;|Truncate table after each successful collection|
|no| |Do not truncate table|

If the table is truncated (default), the metrics are delta counters.
Else, the values are cumulative counters.

### `truncate-timeout`

| | |
|---|---|
|**Value Type**|[Duration string](https://pkg.go.dev/time#ParseDuration)|250ms|
|**Default**|250ms|

Sets `@@session.lock_wait_timeout` to avoid waiting too long when truncating the table.
Normally, truncating a table is nearly instantaneous, but metadata locks can block the operation.

## Group Keys

|Key|Value|
|---|---|
|`db`, `tbl`|Database and table name for table series|
|`db`|Database name for schema series|

## Meta

None.

## Error Policies

|Name|MySQL Error|
|----|-----------|
|`truncate-timeout`|Error truncating table|

## MySQL Config

See
* [29.1 Performance Schema Quick Start](https://dev.mysql.com/doc/refman/en/performance-schema-quick-start.html)
* [10.15.7.1 The table_io_waits_summary_by_table Table](https://dev.mysql.com/doc/mysql-perfschema-excerpt/en/performance-schema-table-io-waits-summary-by-table-table.html)

and related pages in the MySQL manual.

## Changelog

|Blip Version|Change|
|------------|------|
|v1.1.0      |Added `count_star` metric|
|v1.0.0      |Domain added|
