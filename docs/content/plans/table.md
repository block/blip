---
title: "Table"
---

A plan table contains one plan per row:

{{< hint type=note >}}
Plan-table storage is MySQL-specific. The connection configured by a top-level
`plans.monitor` must be MySQL, and external database monitors reject
`plans.table` in their monitor configuration. External database monitors can
use plan files or compatible shared plans.
{{< /hint >}}

```sql
CREATE TABLE IF NOT EXISTS plans (
  name        VARCHAR(100) NOT NULL PRIMARY KEY,
  plan        BLOB NOT NULL,
  monitor_id  VARCHAR(1000) NULL DEFAULT NULL
) ENGINE=InnoDB
```

`name`
: The `name` column is the name of the plan.
It can be any string up to 100 characters long.
The name is important because it's used by [plan precedence]({{< ref "loading#precedence" >}}).

`plan`
: The `plan` column is the full plan in YAML format.
This is the exact same content as a [plan file]({{< ref "file" >}}): spaces, line breaks, and so on.

`monitor_id`
: The `monitor_id` column scopes the plan: if set, it's a [monitor plan]({{< ref "loading#monitor" >}}); if `NULL`, it's a [shared plan]({{< ref "loading#shared" >}}).
The value is a [`config.monitor.id`]({{< ref "/config/config-file#id" >}}).
Since Blip auto-assigns monitor IDs that are not explicitly set, you should explicitly set monitor IDs when using monitor-scoped plans in a table to ensure the two values are equal.

Blip does not create or modify the plan table; you must create it and load the plans (rows).

Grant the necessary [MySQL user privileges]({{< ref "/config/mysql-user#plan-table" >}}) to read the table if necessary.
