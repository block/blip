// Copyright 2024 Block, Inc.

package waitiotable

import (
	"fmt"
	"strings"
)

func TableIoWaitQuery(set map[string]string, metrics []string) (string, []interface{}) {
	columns := setColumns(set, metrics)

	query := fmt.Sprintf("SELECT %s FROM performance_schema.table_io_waits_summary_by_table", strings.Join(columns, ", "))
	var where string
	var params []interface{}
	if include := set[OPT_INCLUDE]; include != "" {
		where, params = setWhere(strings.Split(set[OPT_INCLUDE], ","), true)
	} else {
		where, params = setWhere(strings.Split(set[OPT_EXCLUDE], ","), false)
	}
	return query + where, params
}

// TableIoWaitSchemaQuery rolls up table handler counters by the schema that owns
// each table. The count columns provide the weights for average wait times.
func TableIoWaitSchemaQuery(set map[string]string, metrics []string) (string, []interface{}) {
	columns := setColumns(set, metrics)[2:]
	selectColumns := []string{"OBJECT_SCHEMA", "'' AS OBJECT_NAME"}
	for _, column := range columns {
		switch {
		case strings.HasPrefix(column, "count_"), strings.HasPrefix(column, "sum_timer_"):
			selectColumns = append(selectColumns, fmt.Sprintf("SUM(%s) AS %s", column, column))
		case strings.HasPrefix(column, "min_timer_"):
			selectColumns = append(selectColumns, fmt.Sprintf("COALESCE(MIN(CASE WHEN count_%s > 0 THEN %s END), 0) AS %s", countSuffix(column), column, column))
		case strings.HasPrefix(column, "avg_timer_"):
			operation := strings.TrimPrefix(column, "avg_timer_")
			selectColumns = append(selectColumns, fmt.Sprintf("COALESCE(CAST(SUM(sum_timer_%s) / NULLIF(SUM(count_%s), 0) AS UNSIGNED), 0) AS %s", operation, countSuffix(column), column))
		case strings.HasPrefix(column, "max_timer_"):
			selectColumns = append(selectColumns, fmt.Sprintf("MAX(%s) AS %s", column, column))
		}
	}
	var where string
	var params []interface{}
	if include := set[OPT_INCLUDE]; include != "" {
		where, params = setWhere(strings.Split(include, ","), true)
	} else {
		where, params = setWhere(strings.Split(set[OPT_EXCLUDE], ","), false)
	}
	return fmt.Sprintf("SELECT %s FROM performance_schema.table_io_waits_summary_by_table%s GROUP BY OBJECT_SCHEMA", strings.Join(selectColumns, ", "), where), params
}

func countSuffix(timerColumn string) string {
	operation := timerColumn[strings.LastIndex(timerColumn, "_timer_")+len("_timer_"):]
	if operation == "wait" {
		return "star"
	}
	return operation
}

func setColumns(set map[string]string, metrics []string) []string {
	columns := []string{"OBJECT_SCHEMA", "OBJECT_NAME"}

	if all, ok := set[OPT_ALL]; ok && strings.ToLower(all) == "yes" {
		for _, name := range columnNames {
			columns = append(columns, name)
		}
	} else {
		// Default
		for _, metric := range metrics {
			metric = strings.ToLower(metric)

			if _, ok := columnExists[metric]; ok {
				columns = append(columns, metric)
			}
		}
	}

	return columns
}

func setWhere(tables []string, isInclude bool) (string, []interface{}) {
	where := " WHERE "
	if !isInclude {
		where = where + "NOT "
	}
	var params []interface{} = make([]interface{}, 0)

	for i, excludeTable := range tables {
		if strings.Contains(excludeTable, ".") {
			dbAndTable := strings.Split(excludeTable, ".")
			db := dbAndTable[0]
			table := dbAndTable[1]
			if table == "*" {
				params = append(params, db)
				where = where + "(OBJECT_SCHEMA = ?)"
			} else {
				params = append(params, db, table)
				where = where + "(OBJECT_SCHEMA = ? AND OBJECT_NAME = ?)"
			}
		} else {
			params = append(params, excludeTable)
			where = where + "(OBJECT_NAME = ?)"
		}
		if i != (len(tables) - 1) {
			if isInclude {
				where = where + " OR "
			} else {
				where = where + " AND NOT "
			}
		}
	}
	return where, params
}
