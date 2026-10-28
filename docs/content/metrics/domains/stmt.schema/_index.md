---
title: "stmt.schema"
---

The `stmt.schema` domain sums statement counters from MySQL 8.0 `performance_schema.events_statements_summary_by_digest` by `SCHEMA_NAME`. It is opt-in and does not change the default plan.

## Usage

```yaml
level:
  collect:
    stmt.schema:
      metrics:
        - count_star
        - sum_timer_wait
        - sum_errors
        - sum_rows_examined
      options:
        include: app,other_app
```

The default metrics are `count_star`, `sum_timer_wait`, `sum_errors`, `sum_rows_examined`, and `sum_rows_sent`. Other supported metrics are `sum_lock_time`, `sum_warnings`, `sum_rows_affected`, `sum_created_tmp_tables`, `sum_created_tmp_disk_tables`, `sum_select_scan`, `sum_select_full_join`, `sum_no_index_used`, and `sum_no_good_index_used`.

All values are cumulative counters. Timer values are in picoseconds. Blip's delta sink converts cumulative values to changes between samples. The `db` group contains the statement schema, which does not necessarily identify every table touched by a cross-schema query.

Rows with a null schema or digest are excluded. This includes the digest catch-all row, so the series can undercount when `performance_schema_digests_size` is exhausted. Monitor the catch-all row separately when completeness matters.

## Options

|Option|Default|Description|
|------|-------|-----------|
|`include`||Comma-separated schema names to include; overrides `exclude`|
|`exclude`|`mysql,information_schema,performance_schema,sys`|Comma-separated schema names to exclude|

## Group Keys

|Key|Value|
|---|-----|
|`db`|Statement schema name|

## MySQL Config

Enable Performance Schema and its `statements_digest` consumer. The digest table has a fixed size chosen at startup. A full table sends new digests to a null catch-all row, which this collector cannot assign to a schema.
