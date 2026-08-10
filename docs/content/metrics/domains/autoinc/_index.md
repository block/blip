---
title: "autoinc"
---

The `autoinc` domain reports how much of each auto-increment column's numeric range has been used.

{{< toc >}}

## Usage

The collector reads auto-increment columns from `information_schema.COLUMNS` and `information_schema.TABLES`. It reports a `usage` value from 0 to 1 by dividing the table's next `AUTO_INCREMENT` value by the maximum value of the column's signed or unsigned integer type.

Collect this domain infrequently because auto-increment utilization usually changes slowly. Alert before `usage` reaches 1 so the column can be widened or converted to an unsigned type.

## Derived Metrics

### `usage`

| | |
|---|---|
|**Metric Type**|gauge|
|**Value Units**|ratio|

The fraction of the auto-increment range used.

## Options

### `exclude`

| | |
|---|---|
|**Value Type**|CSV string of db.table|
|**Default**|`mysql.*,information_schema.*,performance_schema.*,sys.*`|

A comma-separated list of database or table names to exclude. This option is ignored when `include` is set.

### `include`

| | |
|---|---|
|**Value Type**|CSV string of db.table|
|**Default**||

A comma-separated list of database or table names to include. This option overrides `exclude`.

## Group Keys

|Key|Value|
|---|---|
|`db`|Database name|
|`tbl`|Table name|
|`col`|Column name|
|`data_type`|Signed or unsigned integer type|

## Meta

None.

## Error Policies

None.

## MySQL Config

None.

## Changelog

|Blip Version|Change|
|------------|------|
|v2.1.0|Domain added|
