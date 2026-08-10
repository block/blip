// Copyright 2024 Block, Inc.

package error

import (
	"testing"

	"github.com/cashapp/blip/v2"
	"github.com/go-test/deep"
)

func TestErrorsSummariesQuery_Account_AllErrors(t *testing.T) {
	baseQuery := BASE_QUERY_ACCOUNT
	groupBy := GROUP_BY_ACCOUNT

	// Generate by account with defaults
	dom := blip.Domain{
		Options: map[string]string{
			OPT_ALL:   "yes",
			OPT_TOTAL: "yes",
		},
		Metrics: []string{"3024", "ER_QUERY_TIMEOUT"},
	}

	got, err := ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_ACCOUNT)
	expect := "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER, HOST FROM performance_schema.events_errors_summary_by_account_by_error WHERE ERROR_NUMBER IS NOT NULL AND USER IS NOT NULL AND HOST IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams := []any{}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Including specific accounts by user
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "yes",
			OPT_EXCLUDE: "",
			OPT_INCLUDE: "user1@*,user2@*",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_ACCOUNT)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER, HOST FROM performance_schema.events_errors_summary_by_account_by_error WHERE ERROR_NUMBER IS NOT NULL AND (USER IN (?, ?)) AND USER IS NOT NULL AND HOST IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{"user1", "user2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Excluding specific accounts by user
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "yes",
			OPT_EXCLUDE: "user1@*,user2@*",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_ACCOUNT)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER, HOST FROM performance_schema.events_errors_summary_by_account_by_error WHERE ERROR_NUMBER IS NOT NULL AND (USER NOT IN (?, ?)) AND USER IS NOT NULL AND HOST IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{"user1", "user2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Including specific accounts by host
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "yes",
			OPT_INCLUDE: "*@host1,*@host2",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_ACCOUNT)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER, HOST FROM performance_schema.events_errors_summary_by_account_by_error WHERE ERROR_NUMBER IS NOT NULL AND (HOST IN (?, ?)) AND USER IS NOT NULL AND HOST IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{"host1", "host2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Excluding specific accounts by host
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "yes",
			OPT_EXCLUDE: "*@host1,*@host2",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_ACCOUNT)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER, HOST FROM performance_schema.events_errors_summary_by_account_by_error WHERE ERROR_NUMBER IS NOT NULL AND (HOST NOT IN (?, ?)) AND USER IS NOT NULL AND HOST IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{"host1", "host2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Including specific accounts by host
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "yes",
			OPT_INCLUDE: "user1@host1,user2@host2",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_ACCOUNT)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER, HOST FROM performance_schema.events_errors_summary_by_account_by_error WHERE ERROR_NUMBER IS NOT NULL AND ((USER, HOST) IN ((?, ?), (?, ?))) AND USER IS NOT NULL AND HOST IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{"user1", "host1", "user2", "host2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Excluding specific accounts by host
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "yes",
			OPT_EXCLUDE: "user1@host1,user2@host2",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_ACCOUNT)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER, HOST FROM performance_schema.events_errors_summary_by_account_by_error WHERE ERROR_NUMBER IS NOT NULL AND ((USER, HOST) NOT IN ((?, ?), (?, ?))) AND USER IS NOT NULL AND HOST IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{"user1", "host1", "user2", "host2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Including a combination of user/host, users, hosts, and invalid inputs
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "yes",
			OPT_INCLUDE: "user1@host1,user3@*,user2@host2,*@host3,*@*,invalid",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_ACCOUNT)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER, HOST FROM performance_schema.events_errors_summary_by_account_by_error WHERE ERROR_NUMBER IS NOT NULL AND ((USER, HOST) IN ((?, ?), (?, ?)) OR USER IN (?) OR HOST IN (?)) AND USER IS NOT NULL AND HOST IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{"user1", "host1", "user2", "host2", "user3", "host3"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Excluding a combination of user/host, users, hosts, and invalid inputs
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "yes",
			OPT_EXCLUDE: "user1@host1,user3@*,user2@host2,*@host3,*@*,invalid",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_ACCOUNT)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER, HOST FROM performance_schema.events_errors_summary_by_account_by_error WHERE ERROR_NUMBER IS NOT NULL AND ((USER, HOST) NOT IN ((?, ?), (?, ?)) AND USER NOT IN (?) AND HOST NOT IN (?)) AND USER IS NOT NULL AND HOST IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{"user1", "host1", "user2", "host2", "user3", "host3"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}
}

func TestErrorsSummariesQuery_Account_SpecificErrors(t *testing.T) {
	baseQuery := BASE_QUERY_ACCOUNT
	groupBy := GROUP_BY_ACCOUNT

	// Generate by account with defaults
	dom := blip.Domain{
		Options: map[string]string{
			OPT_ALL:   "no",
			OPT_TOTAL: "yes",
		},
		Metrics: []string{"3024", "ER_QUERY_TIMEOUT"},
	}

	got, err := ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_ACCOUNT)
	expect := "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER, HOST FROM performance_schema.events_errors_summary_by_account_by_error WHERE (ERROR_NUMBER IN (?) OR ERROR_NAME IN (?)) AND USER IS NOT NULL AND HOST IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams := []any{3024, "ER_QUERY_TIMEOUT"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Including specific accounts by user
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "no",
			OPT_EXCLUDE: "",
			OPT_INCLUDE: "user1@*,user2@*",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{"3024", "ER_QUERY_TIMEOUT"},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_ACCOUNT)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER, HOST FROM performance_schema.events_errors_summary_by_account_by_error WHERE (ERROR_NUMBER IN (?) OR ERROR_NAME IN (?)) AND (USER IN (?, ?)) AND USER IS NOT NULL AND HOST IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{3024, "ER_QUERY_TIMEOUT", "user1", "user2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Including a combination of user/host, users, hosts, and invalid inputs
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "no",
			OPT_INCLUDE: "user1@host1,user3@*,user2@host2,*@host3,*@*,invalid",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{"3024", "ER_QUERY_TIMEOUT"},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_ACCOUNT)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER, HOST FROM performance_schema.events_errors_summary_by_account_by_error WHERE (ERROR_NUMBER IN (?) OR ERROR_NAME IN (?)) AND ((USER, HOST) IN ((?, ?), (?, ?)) OR USER IN (?) OR HOST IN (?)) AND USER IS NOT NULL AND HOST IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{3024, "ER_QUERY_TIMEOUT", "user1", "host1", "user2", "host2", "user3", "host3"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}
}

func TestErrorsSummariesQuery_Global(t *testing.T) {
	baseQuery := BASE_QUERY_GLOBAL
	groupBy := GROUP_BY_GLOBAL

	// All defaults
	dom := blip.Domain{
		Options: map[string]string{
			OPT_ALL:   "yes",
			OPT_TOTAL: "yes",
		},
		Metrics: []string{},
	}

	got, err := ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_GLOBAL)
	expect := "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME FROM performance_schema.events_errors_summary_global_by_error WHERE ERROR_NUMBER IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams := []any{}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Specific errors with defaults
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:   "no",
			OPT_TOTAL: "yes",
		},
		Metrics: []string{"3024", "ER_QUERY_TIMEOUT"},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_GLOBAL)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME FROM performance_schema.events_errors_summary_global_by_error WHERE (ERROR_NUMBER IN (?) OR ERROR_NAME IN (?)) AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{3024, "ER_QUERY_TIMEOUT"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Exclude specific errors
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:   "exclude",
			OPT_TOTAL: "yes",
		},
		Metrics: []string{"3024", "ER_QUERY_TIMEOUT"},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_GLOBAL)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME FROM performance_schema.events_errors_summary_global_by_error WHERE (ERROR_NUMBER NOT IN (?) AND ERROR_NAME NOT IN (?)) AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{3024, "ER_QUERY_TIMEOUT"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Collect all error and emit only the total
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:   "yes",
			OPT_TOTAL: "only",
		},
		Metrics: []string{},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_GLOBAL)
	expect = "SELECT SUM(SUM_ERROR_RAISED), '' ERROR_NUMBER, '' ERROR_NAME FROM performance_schema.events_errors_summary_global_by_error WHERE ERROR_NUMBER IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}
}

func TestErrorsSummariesQuery_Host(t *testing.T) {
	baseQuery := BASE_QUERY_HOST
	groupBy := GROUP_BY_HOST

	// Generate by host with defaults
	dom := blip.Domain{
		Options: map[string]string{
			OPT_ALL:   "yes",
			OPT_TOTAL: "yes",
		},
		Metrics: []string{},
	}

	got, err := ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_HOST)
	expect := "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, HOST FROM performance_schema.events_errors_summary_by_host_by_error WHERE ERROR_NUMBER IS NOT NULL AND HOST IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams := []any{}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Generate by host with specific errors
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:   "no",
			OPT_TOTAL: "yes",
		},
		Metrics: []string{"3024", "ER_QUERY_TIMEOUT"},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_HOST)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, HOST FROM performance_schema.events_errors_summary_by_host_by_error WHERE (ERROR_NUMBER IN (?) OR ERROR_NAME IN (?)) AND HOST IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{3024, "ER_QUERY_TIMEOUT"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Including specific hosts
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "no",
			OPT_EXCLUDE: "event_scheduler",
			OPT_INCLUDE: "host1,host2",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{"3024", "ER_QUERY_TIMEOUT"},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_HOST)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, HOST FROM performance_schema.events_errors_summary_by_host_by_error WHERE (ERROR_NUMBER IN (?) OR ERROR_NAME IN (?)) AND HOST IN (?, ?) AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{3024, "ER_QUERY_TIMEOUT", "host1", "host2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Including specific hosts but all errors
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "all",
			OPT_EXCLUDE: "event_scheduler",
			OPT_INCLUDE: "host1,host2",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_HOST)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, HOST FROM performance_schema.events_errors_summary_by_host_by_error WHERE ERROR_NUMBER IS NOT NULL AND HOST IN (?, ?) AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{"host1", "host2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Including specific hosts but all errors
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "all",
			OPT_EXCLUDE: "event_scheduler",
			OPT_INCLUDE: "host1,host2",
			OPT_TOTAL:   "only",
		},
		Metrics: []string{},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_HOST)
	expect = "SELECT SUM(SUM_ERROR_RAISED), '' ERROR_NUMBER, '' ERROR_NAME, HOST FROM performance_schema.events_errors_summary_by_host_by_error WHERE ERROR_NUMBER IS NOT NULL AND HOST IN (?, ?) AND SUM_ERROR_RAISED > 0 GROUP BY HOST"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{"host1", "host2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}
}

func TestErrorsSummariesQuery_User(t *testing.T) {
	baseQuery := BASE_QUERY_USER
	groupBy := GROUP_BY_USER

	// Generate by user with defaults
	dom := blip.Domain{
		Options: map[string]string{
			OPT_ALL:   "yes",
			OPT_TOTAL: "yes",
		},
		Metrics: []string{},
	}

	got, err := ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_USER)
	expect := "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER FROM performance_schema.events_errors_summary_by_user_by_error WHERE ERROR_NUMBER IS NOT NULL AND USER IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams := []any{}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Generate by user with specific errors
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:   "no",
			OPT_TOTAL: "yes",
		},
		Metrics: []string{"3024", "ER_QUERY_TIMEOUT"},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_USER)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER FROM performance_schema.events_errors_summary_by_user_by_error WHERE (ERROR_NUMBER IN (?) OR ERROR_NAME IN (?)) AND USER IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{3024, "ER_QUERY_TIMEOUT"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Including specific users
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "no",
			OPT_INCLUDE: "user1,user2",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{"3024", "ER_QUERY_TIMEOUT"},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_USER)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER FROM performance_schema.events_errors_summary_by_user_by_error WHERE (ERROR_NUMBER IN (?) OR ERROR_NAME IN (?)) AND USER IN (?, ?) AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{3024, "ER_QUERY_TIMEOUT", "user1", "user2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Excluding specific users
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "no",
			OPT_EXCLUDE: "user1,user2",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{"3024", "ER_QUERY_TIMEOUT"},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_USER)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, USER FROM performance_schema.events_errors_summary_by_user_by_error WHERE (ERROR_NUMBER IN (?) OR ERROR_NAME IN (?)) AND USER NOT IN (?, ?) AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{3024, "ER_QUERY_TIMEOUT", "user1", "user2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Including specific users and only emit totals
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "all",
			OPT_INCLUDE: "user1,user2",
			OPT_TOTAL:   "only",
		},
		Metrics: []string{},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_USER)
	expect = "SELECT SUM(SUM_ERROR_RAISED), '' ERROR_NUMBER, '' ERROR_NAME, USER FROM performance_schema.events_errors_summary_by_user_by_error WHERE ERROR_NUMBER IS NOT NULL AND USER IN (?, ?) AND SUM_ERROR_RAISED > 0 GROUP BY USER"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{"user1", "user2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}
}

func TestErrorsSummariesQuery_Thread(t *testing.T) {
	baseQuery := BASE_QUERY_THREAD
	groupBy := GROUP_BY_THREAD

	// Generate by thread with defaults
	dom := blip.Domain{
		Options: map[string]string{
			OPT_ALL:   "yes",
			OPT_TOTAL: "yes",
		},
		Metrics: []string{},
	}

	got, err := ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_THREAD)
	expect := "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, THREAD_ID FROM performance_schema.events_errors_summary_by_thread_by_error WHERE ERROR_NUMBER IS NOT NULL AND THREAD_ID IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams := []any{}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Generate by thread with specific errors
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:   "no",
			OPT_TOTAL: "yes",
		},
		Metrics: []string{"3024", "ER_QUERY_TIMEOUT"},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_THREAD)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, THREAD_ID FROM performance_schema.events_errors_summary_by_thread_by_error WHERE (ERROR_NUMBER IN (?) OR ERROR_NAME IN (?)) AND THREAD_ID IS NOT NULL AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{3024, "ER_QUERY_TIMEOUT"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Including specific threads
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "no",
			OPT_INCLUDE: "1,2",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{"3024", "ER_QUERY_TIMEOUT"},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_THREAD)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, THREAD_ID FROM performance_schema.events_errors_summary_by_thread_by_error WHERE (ERROR_NUMBER IN (?) OR ERROR_NAME IN (?)) AND THREAD_ID IN (?, ?) AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{3024, "ER_QUERY_TIMEOUT", "1", "2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Excluding specific threads
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "no",
			OPT_EXCLUDE: "1,2",
			OPT_TOTAL:   "yes",
		},
		Metrics: []string{"3024", "ER_QUERY_TIMEOUT"},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_THREAD)
	expect = "SELECT SUM_ERROR_RAISED, ERROR_NUMBER, ERROR_NAME, THREAD_ID FROM performance_schema.events_errors_summary_by_thread_by_error WHERE (ERROR_NUMBER IN (?) OR ERROR_NAME IN (?)) AND THREAD_ID NOT IN (?, ?) AND SUM_ERROR_RAISED > 0"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{3024, "ER_QUERY_TIMEOUT", "1", "2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}

	// Including specific threads and only emit totals
	dom = blip.Domain{
		Options: map[string]string{
			OPT_ALL:     "all",
			OPT_INCLUDE: "1,2",
			OPT_TOTAL:   "only",
		},
		Metrics: []string{},
	}

	got, err = ErrorsQuery(dom, baseQuery, groupBy, SUB_DOMAIN_THREAD)
	expect = "SELECT SUM(SUM_ERROR_RAISED), '' ERROR_NUMBER, '' ERROR_NAME, THREAD_ID FROM performance_schema.events_errors_summary_by_thread_by_error WHERE ERROR_NUMBER IS NOT NULL AND THREAD_ID IN (?, ?) AND SUM_ERROR_RAISED > 0 GROUP BY THREAD_ID"
	if err != nil {
		t.Error(err)
	}
	if got.query != expect {
		t.Errorf("got:\n%s\nexpect:\n%s\n", got.query, expect)
	}

	expectedParams = []any{"1", "2"}
	if diff := deep.Equal(got.params, expectedParams); diff != nil {
		t.Error(diff)
	}
}
