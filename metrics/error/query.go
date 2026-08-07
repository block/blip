// Copyright 2024 Block, Inc.

package error

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/sqlutil"
)

// ErrorsQuery builds a query for error metrics based on the domain configuration.
// The base query and groupBy parameters allow customization for different error domains.
func ErrorsQuery(dom blip.Domain, baseQuery string, groupBy string, subDomain string) (*errorLevelOptions, error) {
	var query string
	var grpBy string
	query = baseQuery

	if strings.ToLower(dom.Options[OPT_TOTAL]) == "only" {
		// Modify the query to get totals only
		query = strings.Replace(baseQuery, "ERROR_NUMBER, ERROR_NAME", "'' ERROR_NUMBER, '' ERROR_NAME", 1)
		query = strings.Replace(query, "SUM_ERROR_RAISED", "SUM(SUM_ERROR_RAISED)", 1)
		grpBy = groupBy
	}

	where, params, err := setWhere(dom, subDomain)
	if err != nil {
		return nil, err
	}

	return &errorLevelOptions{
		query:  query + where + grpBy,
		params: params,
	}, nil
}

// setAnd is a helper function to determine if the "AND" clause should be added to the WHERE clause.
func setAnd(where strings.Builder) string {
	if where.Len() == 0 {
		return ""
	} else {
		return " AND"
	}
}

func setWhere(dom blip.Domain, subDomain string) (string, []any, error) {
	// Initialize the where clause once if we need it
	var where strings.Builder
	var params []any = make([]any, 0)
	var errorNum []any = make([]any, 0)
	var errorName []any = make([]any, 0)

	if strings.ToLower(dom.Options[OPT_ALL]) != "yes" && len(dom.Metrics) > 0 {
		var inCond, conj string
		switch strings.ToLower(dom.Options[OPT_ALL]) {
		case "no":
			inCond = "IN"
			conj = " OR "
		case "exclude":
			inCond = "NOT IN"
			conj = " AND "
		default:
			return "", nil, fmt.Errorf("invalid option %s", dom.Options[OPT_ALL])
		}

		where.WriteString(setAnd(where))

		for _, metric := range dom.Metrics {
			if i, err := strconv.Atoi(metric); err == nil {
				errorNum = append(errorNum, i)
			} else {
				errorName = append(errorName, metric)
			}
		}

		errorConditions := make([]string, 0, 2)
		if len(errorNum) > 0 {
			errorConditions = append(errorConditions, fmt.Sprintf("ERROR_NUMBER %s (%s)", inCond, sqlutil.PlaceholderList(len(errorNum))))
			params = append(params, errorNum...)
		}

		if len(errorName) > 0 {
			errorConditions = append(errorConditions, fmt.Sprintf("ERROR_NAME %s (%s)", inCond, sqlutil.PlaceholderList(len(errorName))))
			params = append(params, errorName...)
		}

		where.WriteString(fmt.Sprintf(" (%s)", strings.Join(errorConditions, conj)))
	} else {
		// Exclude NULL error numbers
		where.WriteString(setAnd(where))
		where.WriteString(" ERROR_NUMBER IS NOT NULL")
	}

	// Handle include/exclude filters based on the collector type
	var subWhere string
	var subParams []any = make([]any, 0)

	switch subDomain {
	case SUB_DOMAIN_ACCOUNT:
		subWhere, subParams = getAccountFilters(dom)
	case SUB_DOMAIN_USER:
		subWhere, subParams = getUserFilters(dom)
	case SUB_DOMAIN_HOST:
		subWhere, subParams = getHostFilters(dom)
	case SUB_DOMAIN_THREAD:
		subWhere, subParams = getThreadFilters(dom)
	case SUB_DOMAIN_GLOBAL:
		// No additional filters needed
	}

	if len(subWhere) > 0 {
		where.WriteString(setAnd(where))
		where.WriteString(subWhere)
		params = append(params, subParams...)
	}

	// Exclude errors without any errors raised
	where.WriteString(setAnd(where))
	where.WriteString(" SUM_ERROR_RAISED > 0")

	return " WHERE" + where.String(), params, nil
}

func getAccountFilters(dom blip.Domain) (string, []any) {
	var where strings.Builder
	var params []any = []any{}
	hasValidTokens := false
	var conjunction, inOp string
	onlyUser := []string{}
	onlyHost := []string{}
	onlyAccount := [][]string{}

	splitTokens := func(str string) {
		tokens := strings.Split(str, ",")

		for _, token := range tokens {
			parts := strings.Split(token, "@")
			// Only process accounts that are in the format user@host.
			// Wildcards are allowed for either user or host but not both.
			if len(parts) == 2 {
				if parts[0] == "*" && parts[1] != "*" {
					onlyHost = append(onlyHost, parts[1])
					hasValidTokens = true
				} else if parts[0] != "*" && parts[1] == "*" {
					onlyUser = append(onlyUser, parts[0])
					hasValidTokens = true
				} else if parts[0] != "*" && parts[1] != "*" {
					// This is an account with a specific user and host
					onlyAccount = append(onlyAccount, parts)
					hasValidTokens = true
				}
			}
		}
	}

	if include := dom.Options[OPT_INCLUDE]; include != "" {
		inOp = "IN"
		conjunction = "OR"
		splitTokens(include)
	} else if exclude := dom.Options[OPT_EXCLUDE]; exclude != "" {
		inOp = "NOT IN"
		conjunction = "AND"
		splitTokens(exclude)
	}

	if hasValidTokens {
		where.WriteString(setAnd(where))
		where.WriteString(" (")
		parts := make([]string, 0, 3)

		if len(onlyAccount) > 0 {
			parts = append(parts, fmt.Sprintf("(USER, HOST) %s (%s)", inOp, sqlutil.MultiPlaceholderList(len(onlyAccount), 2)))
			for _, tokens := range onlyAccount {
				params = append(params, sqlutil.ToInterfaceArray(tokens)...)
			}
		}

		if len(onlyUser) > 0 {
			parts = append(parts, fmt.Sprintf("USER %s (%s)", inOp, sqlutil.PlaceholderList(len(onlyUser))))
			params = append(params, sqlutil.ToInterfaceArray(onlyUser)...)
		}

		if len(onlyHost) > 0 {
			parts = append(parts, fmt.Sprintf("HOST %s (%s)", inOp, sqlutil.PlaceholderList(len(onlyHost))))
			params = append(params, sqlutil.ToInterfaceArray(onlyHost)...)
		}

		where.WriteString(strings.Join(parts, fmt.Sprintf(" %s ", conjunction)))
		where.WriteString(")")
	}

	// Ensure user and host are not NULL
	where.WriteString(setAnd(where))
	where.WriteString(" USER IS NOT NULL AND HOST IS NOT NULL")

	return where.String(), params
}

func getUserFilters(dom blip.Domain) (string, []any) {
	var where strings.Builder
	var params []any = []any{}

	if include := dom.Options[OPT_INCLUDE]; include != "" {
		where.WriteString(setAnd(where))
		users := strings.Split(include, ",")

		where.WriteString(fmt.Sprintf(" USER IN (%s)", sqlutil.PlaceholderList(len(users))))
		params = append(params, sqlutil.ToInterfaceArray(users)...)
	} else if exclude := dom.Options[OPT_EXCLUDE]; exclude != "" {
		where.WriteString(setAnd(where))
		users := strings.Split(exclude, ",")
		where.WriteString(fmt.Sprintf(" USER NOT IN (%s)", sqlutil.PlaceholderList(len(users))))
		params = append(params, sqlutil.ToInterfaceArray(users)...)
	} else {
		// Exclude NULL users
		where.WriteString(setAnd(where))
		where.WriteString(" USER IS NOT NULL")
	}

	return where.String(), params
}

func getHostFilters(dom blip.Domain) (string, []any) {
	var where strings.Builder
	var params []any = []any{}

	if include := dom.Options[OPT_INCLUDE]; include != "" {
		where.WriteString(setAnd(where))
		hosts := strings.Split(include, ",")

		where.WriteString(fmt.Sprintf(" HOST IN (%s)", sqlutil.PlaceholderList(len(hosts))))
		params = append(params, sqlutil.ToInterfaceArray(hosts)...)
	} else if exclude := dom.Options[OPT_EXCLUDE]; exclude != "" {
		where.WriteString(setAnd(where))
		hosts := strings.Split(exclude, ",")
		where.WriteString(fmt.Sprintf(" HOST NOT IN (%s)", sqlutil.PlaceholderList(len(hosts))))
		params = append(params, sqlutil.ToInterfaceArray(hosts)...)
	} else {
		// Exclude NULL hosts
		where.WriteString(setAnd(where))
		where.WriteString(" HOST IS NOT NULL")
	}

	return where.String(), params
}

func getThreadFilters(dom blip.Domain) (string, []any) {
	var where strings.Builder
	var params []any = []any{}

	if include := dom.Options[OPT_INCLUDE]; include != "" {
		where.WriteString(setAnd(where))
		threads := strings.Split(include, ",")
		where.WriteString(fmt.Sprintf(" THREAD_ID IN (%s)", sqlutil.PlaceholderList(len(threads))))
		params = append(params, sqlutil.ToInterfaceArray(threads)...)
	} else if exclude := dom.Options[OPT_EXCLUDE]; exclude != "" {
		where.WriteString(setAnd(where))
		threads := strings.Split(exclude, ",")
		where.WriteString(fmt.Sprintf(" THREAD_ID NOT IN (%s)", sqlutil.PlaceholderList(len(threads))))
		params = append(params, sqlutil.ToInterfaceArray(threads)...)
	} else {
		// Exclude NULL thread IDs
		where.WriteString(setAnd(where))
		where.WriteString(" THREAD_ID IS NOT NULL")
	}

	return where.String(), params
}
