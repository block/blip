// Copyright 2024 Block, Inc.

package tls

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/test"
)

type queryResult struct {
	query string
	args  []interface{}
	value string
	err   error
}

type fakeQueryRows struct {
	t       *testing.T
	results []queryResult
	n       int
}

func (f *fakeQueryRows) queryRow(ctx context.Context, query string, args ...interface{}) rowScanner {
	f.t.Helper()
	if f.n >= len(f.results) {
		f.t.Fatalf("unexpected query %q with args %v", query, args)
	}

	result := f.results[f.n]
	f.n++
	if query != result.query {
		f.t.Fatalf("query %d = %q, expected %q", f.n, query, result.query)
	}
	if !reflect.DeepEqual(args, result.args) {
		f.t.Fatalf("query %d args = %v, expected %v", f.n, args, result.args)
	}
	return fakeRow{value: result.value, err: result.err}
}

func (f *fakeQueryRows) verify(t *testing.T) {
	t.Helper()
	if f.n != len(f.results) {
		t.Fatalf("executed %d queries, expected %d", f.n, len(f.results))
	}
}

type fakeRow struct {
	value string
	err   error
}

func (r fakeRow) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 1 {
		return fmt.Errorf("scan destinations = %d, expected 1", len(dest))
	}
	value, ok := dest[0].(*string)
	if !ok {
		return fmt.Errorf("scan destination is %T, expected *string", dest[0])
	}
	*value = r.value
	return nil
}

func tlsPlan(metrics ...string) blip.Plan {
	return blip.Plan{
		Levels: map[string]blip.Level{
			"kpi": {
				Name: "kpi",
				Collect: map[string]blip.Domain{
					DOMAIN: {
						Name:    DOMAIN,
						Metrics: metrics,
					},
				},
			},
		},
	}
}

func TestCollectFromTLSChannelStatus(t *testing.T) {
	fake := &fakeQueryRows{
		t: t,
		results: []queryResult{
			{query: tlsChannelStatusQuery, args: []interface{}{"mysql_main", "Enabled"}, value: "Yes"},
			{query: tlsChannelStatusQuery, args: []interface{}{"mysql_main", "Enabled"}, value: "No"},
		},
	}
	c := &TLS{queryRow: fake.queryRow}

	if _, err := c.Prepare(context.Background(), tlsPlan("enabled")); err != nil {
		t.Fatal(err)
	}
	metrics, err := c.Collect(context.Background(), "kpi")
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 || metrics[0].Name != "enabled" || metrics[0].Value != 0 {
		t.Fatalf("metrics = %+v, expected tls.enabled = 0", metrics)
	}
	fake.verify(t)
}

func TestCollectFallsBackWhenTLSChannelStatusDoesNotExist(t *testing.T) {
	fake := &fakeQueryRows{
		t: t,
		results: []queryResult{
			{
				query: tlsChannelStatusQuery,
				args:  []interface{}{"mysql_main", "Enabled"},
				err:   &mysql.MySQLError{Number: 1146, Message: "table does not exist"},
			},
			{query: haveSSLQuery, value: "YES"},
			{query: haveSSLQuery, value: "DISABLED"},
		},
	}
	c := &TLS{queryRow: fake.queryRow}

	if _, err := c.Prepare(context.Background(), tlsPlan("enabled")); err != nil {
		t.Fatal(err)
	}
	metrics, err := c.Collect(context.Background(), "kpi")
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 || metrics[0].Name != "enabled" || metrics[0].Value != 0 {
		t.Fatalf("metrics = %+v, expected tls.enabled = 0", metrics)
	}
	fake.verify(t)
}

func TestCollectFallsBackWhenTLSChannelStatusRowIsAbsent(t *testing.T) {
	fake := &fakeQueryRows{
		t: t,
		results: []queryResult{
			{
				query: tlsChannelStatusQuery,
				args:  []interface{}{"mysql_main", "Enabled"},
				err:   sql.ErrNoRows,
			},
			{query: haveSSLQuery, value: "YES"},
			{query: haveSSLQuery, value: "YES"},
		},
	}
	c := &TLS{queryRow: fake.queryRow}

	if _, err := c.Prepare(context.Background(), tlsPlan("enabled")); err != nil {
		t.Fatal(err)
	}
	metrics, err := c.Collect(context.Background(), "kpi")
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 || metrics[0].Name != "enabled" || metrics[0].Value != 1 {
		t.Fatalf("metrics = %+v, expected tls.enabled = 1", metrics)
	}
	fake.verify(t)
}

func TestCollectFallsBackWhenTLSChannelStatusAccessIsDenied(t *testing.T) {
	fake := &fakeQueryRows{
		t: t,
		results: []queryResult{
			{
				query: tlsChannelStatusQuery,
				args:  []interface{}{"mysql_main", "Enabled"},
				err:   &mysql.MySQLError{Number: 1142, Message: "SELECT command denied"},
			},
			{query: haveSSLQuery, value: "YES"},
			{query: haveSSLQuery, value: "YES"},
		},
	}
	c := &TLS{queryRow: fake.queryRow}

	if _, err := c.Prepare(context.Background(), tlsPlan("enabled")); err != nil {
		t.Fatal(err)
	}
	metrics, err := c.Collect(context.Background(), "kpi")
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 || metrics[0].Name != "enabled" || metrics[0].Value != 1 {
		t.Fatalf("metrics = %+v, expected tls.enabled = 1", metrics)
	}
	fake.verify(t)
}

func TestPrepareReportsRequiredTLSChannelStatusAccess(t *testing.T) {
	fake := &fakeQueryRows{
		t: t,
		results: []queryResult{
			{
				query: tlsChannelStatusQuery,
				args:  []interface{}{"mysql_main", "Enabled"},
				err:   &mysql.MySQLError{Number: 1142, Message: "SELECT command denied"},
			},
			{
				query: haveSSLQuery,
				err:   &mysql.MySQLError{Number: 1193, Message: "Unknown system variable 'have_ssl'"},
			},
		},
	}
	c := &TLS{queryRow: fake.queryRow}

	_, err := c.Prepare(context.Background(), tlsPlan("enabled"))
	if err == nil {
		t.Fatal("Prepare error = nil, expected access error")
	}
	if got := err.Error(); !strings.Contains(got, "grant SELECT on performance_schema.tls_channel_status") {
		t.Fatalf("Prepare error = %q, expected SELECT requirement", got)
	}
	fake.verify(t)
}

func TestPrepareDoesNotHideTLSChannelStatusErrors(t *testing.T) {
	fake := &fakeQueryRows{
		t: t,
		results: []queryResult{
			{
				query: tlsChannelStatusQuery,
				args:  []interface{}{"mysql_main", "Enabled"},
				err:   errors.New("access denied"),
			},
		},
	}
	c := &TLS{queryRow: fake.queryRow}

	_, err := c.Prepare(context.Background(), tlsPlan("enabled"))
	if err == nil {
		t.Fatal("Prepare error = nil, expected access error")
	}
	fake.verify(t)
}

func TestCollectMySQL80And84(t *testing.T) {
	for _, version := range []string{"mysql80", "mysql84"} {
		t.Run(version, func(t *testing.T) {
			_, db, err := test.Connection(version)
			if err != nil {
				if test.Build {
					t.Skipf("%s not running", version)
				}
				t.Fatal(err)
			}
			defer db.Close()

			c := NewTLS(db)
			if _, err := c.Prepare(context.Background(), tlsPlan("enabled")); err != nil {
				t.Fatal(err)
			}
			metrics, err := c.Collect(context.Background(), "kpi")
			if err != nil {
				t.Fatal(err)
			}
			if len(metrics) != 1 || metrics[0].Name != "enabled" || metrics[0].Value != 1 {
				t.Fatalf("metrics = %+v, expected tls.enabled = 1", metrics)
			}
		})
	}
}
