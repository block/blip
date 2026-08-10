---
weight: 100
---

Following are _all_ Blip domains and the metrics collected in each.
Only domains with a Blip version are collected.
The rest are reserved for future use.

|Domain|Metrics|Blip Version|
|:-----|:------|:-----------|
|access|Access statistics||
|access.index|Index access statistics (`sys.schema_index_statistics`)||
|access.table|Table access statistics (`sys.schema_table_statistics`)||
|aria|MariaDB Aria storage engine||
|[`autoinc`](domains#autoinc)|Auto-increment column utilization|v2.1.0|
|aws|Amazon Web Services||
|[`aws.rds`](domains#awsrds)|[Amazon RDS metrics](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/monitoring-cloudwatch.html#rds-metrics)|v1.0.0|
|aws.aurora|Amazon Aurora||
|azure|Microsoft Azure||
|error|MySQL, client, and query errors||
|error.client|Client errors||
|[error.account](domains#error.account)|Error counts and rates by account [`Error Summary Tables`](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-summary-tables.html)|v2.1.0|
|[error.global](domains#error.global)|Global error counts and rates [`Error Summary Tables`](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-summary-tables.html)|v2.1.0|
|[error.host](domains#error.host)|Error counts and rates by host [`Error Summary Tables`](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-summary-tables.html)|v2.1.0|
|[error.thread](domains#error.thread)|Error counts and rates by thread [`Error Summary Tables`](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-summary-tables.html)|v2.1.0|
|[error.user](domains#error.user)|Error counts and rates by user [`Error Summary Tables`](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-summary-tables.html)|v2.1.0|
|error.query|Query errors||
|error.repl|Replication errors||
|event|[MySQL Event Scheduler](https://dev.mysql.com/doc/refman/8.0/en/event-scheduler.html)||
|file|Files and tablespaces||
|galera|Percona XtraDB Cluster and MariaDB Cluster (wsrep)||
|gcp|Google Cloud||
|gr|MySQL Group Replication||
|host|Host (client)||
|[`innodb`](domains#innodb)|InnoDB metrics [`INFORMATION_SCHEMA.INNODB_METRICS`](https://dev.mysql.com/doc/refman/en/information-schema-innodb-metrics-table.html)|v1.0.0|
|[`innodb.buffer-pool`](domains#innodbbuffer-pool)|InnoDB buffer pool metrics [`INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS`](https://dev.mysql.com/doc/refman/8.4/en/information-schema-innodb-buffer-pool-stats-table.html)|v2.1.0|
|innodb.mutex|InnoDB mutexes `SHOW ENGINE INNODB MUTEX`||
|mariadb|MariaDB enhancements||
|ndb|MySQL NDB Cluster||
|oracle|Oracle enhancements||
|percona|Percona Server enhancements||
|[`percona.response-time`](domains#perconaresponse-time)|Percona Server 5.7 Response Time Distribution plugin|v1.0.0|
|perconca.userstat|[Percona User Statistics](https://www.percona.com/doc/percona-server/8.0/diagnostics/user_stats.html)||
|percona.userstat.index|Percona `userstat` index statistics (`INFORMATION_SCHEMA.INDEX_STATISTICS`)|
|percona.userstat.table|Percona `userstat` table statistics||
|processlist|Processlist `SHOW PROCESSLIST` or `INFORMATION_SCHEMA.PROCESSLIST`||
|pfs|Performance Schema `SHOW ENGINE PERFORMANCE_SCHEMA STATUS`||
|pxc|Percona XtraDB Cluster||
|query|Query metrics||
|[`query.response-time`](domains#queryresponse-time)|Global query response time (MySQL 8.0)|v1.0.0|
|[`repl`](domains#repl)|MySQL replication `SHOW SLAVE|REPLICA STATUS`|v1.0.0|
|[`repl.lag`](domains#repllag)|MySQL replication lag (including heartbeats)|v1.0.0|
|rocksdb|RocksDB store engine||
|size|Storage sizes (in bytes)||
|[`size.binlog`](domains#sizebinlog)|Binary log size|v1.0.0|
|[`size.database`](domains#sizedatabase)|Database sizes|v1.0.0|
|size.file|File sizes (`innodb_undo` and `innodb_temp`)||
|size.index|Index sizes||
|[`size.table`](domains#sizetable)|Table sizes|v1.0.0|
|stage|Statement execution stages||
|status.account|Status by account||
|[`status.global`](domains#statusglobal)|Global status variables `SHOW GLOBAL STATUS`|v1.0.0|
|status.host|Status by host||
|status.thread|Status by thread||
|status.user|Status by user||
|stmt|Statements||
|[`stmt.current`](domains#stmtcurrent)|Current statements|v1.0.0|
|stmt.history|Historical statements||
|thd|Threads||
|[`tls`](domains#tls)|TLS (SSL) status and configuration|v1.0.0|
|tokudb|TokuDB storage engine||
|[`trx`](domains#trx)|Transactions|v1.0.0|
|[`var.global`](domains#varglobal)|MySQL global system variables (sysvars) `SHOW GLOBAL VARIABLES`|v1.0.0|
|wait|Stage waits||
|wait.current|Current waits||
|wait.history|Historical waits||
|[`wait.io.table`](domains#waitiotable)|Table I/O wait metrics [`performance_schema.table_io_waits_summary_by_table`](https://dev.mysql.com/doc/refman/en/performance-schema-table-wait-summary-tables.html#performance-schema-table-io-waits-summary-by-table-table)|v1.0.0|
