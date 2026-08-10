// Copyright 2024 Block, Inc.

package sizedatabase

import (
	"fmt"
	"strings"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/sqlutil"
)

func DataSizeQuery(set map[string]string, def blip.CollectorHelp) (string, []interface{}, error) {
	cols := ""
	groupBy := ""
	if val := set[OPT_TOTAL]; val == "only" {
		cols = "\"\" AS db"
	} else {
		cols = "table_schema AS db"
		groupBy = " GROUP BY 1"
	}
	cols += ", SUM(data_length+index_length) AS bytes"

	like := false
	if val := set[OPT_LIKE]; val == "yes" {
		like = true
	}

	var params []interface{}

	where := ""
	if include := set[OPT_INCLUDE]; include != "" {
		o := strings.Split(include, ",")
		params = make([]interface{}, 0, len(o))

		if like {
			for i := range o {
				params = append(params, o[i])
				o[i] = "table_schema LIKE ?"
			}
			where = strings.Join(o, " OR ")
		} else {
			where = fmt.Sprintf("table_schema IN (%s)", sqlutil.PlaceholderList(len(o)))
			params = sqlutil.ToInterfaceArray(o)
		}
	} else {
		exclude := set[OPT_EXCLUDE]
		if exclude == "" {
			exclude = def.Options[OPT_EXCLUDE].Default
		}
		o := strings.Split(exclude, ",")
		params = make([]interface{}, 0, len(o))

		if like {
			for i := range o {
				params = append(params, o[i])
				o[i] = "table_schema NOT LIKE ?"
			}
			where = strings.Join(o, " AND ")
		} else {
			where = fmt.Sprintf("table_schema NOT IN (%s)", sqlutil.PlaceholderList(len(o)))
			params = sqlutil.ToInterfaceArray(o)
		}
	}

	return "SELECT " + cols + " FROM information_schema.tables WHERE " + where + groupBy, params, nil
}
