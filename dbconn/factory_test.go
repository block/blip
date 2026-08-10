// Copyright 2024 Block, Inc.

package dbconn_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/dbconn"
	"github.com/cashapp/blip/v2/test"
)

func sysvar(db *sql.DB, name string) (string, error) {
	var val string
	err := db.QueryRow("SELECT @@" + name).Scan(&val)
	return val, err
}

// --------------------------------------------------------------------------
func TestConnect(t *testing.T) {
	if _, _, err := test.Connection(test.DefaultMySQLVersion); err != nil {
		if test.Build {
			t.Skip("mysql57 not running")
		} else {
			t.Fatal(err)
		}
	}

	// The most basic functionality: connect to the MySQL instance in Docker
	called := false
	modifyDB := func(*sql.DB, string) {
		called = true
	}
	f := dbconn.NewConnFactory(nil, modifyDB)

	// Minimal config: username, password, and address with the special test port
	cfg := blip.ConfigMonitor{
		Username: "root",
		Password: "test",
		Hostname: "127.0.0.1:" + test.MySQLPort[test.DefaultMySQLVersion],
	}

	// Make makes the connection (sql.DB) or returns an error
	db, dsn, err := f.Make(cfg)
	if err != nil {
		t.Error(err)
	}
	if db == nil {
		t.Fatal("got nil *sql.DB, execpted non-nil value (no return error)")
	}
	defer db.Close()

	// Make returns a print-safe DSN: password ("test") replaced with "..."
	expectDSN := fmt.Sprintf("%s:...@tcp(%s)/?interpolateParams=true&parseTime=true", cfg.Username, cfg.Hostname)
	if dsn != expectDSN {
		t.Errorf("got DSN '%s', expected '%s'", dsn, expectDSN)
	}

	// Verify that the conn (sql.DB) truly works by querying MySQL for a simple
	// SELECT @@version which should return some string containing "8.0" in all
	// cases. The actual string can vary like "8.0.41-community" and such, but
	// if we're truly connect to the MySQL 8.0 test instance, then @@version must
	// contain at least "8.0".
	val, err := sysvar(db, "version")
	if err != nil {
		t.Error(err)
	}
	if !strings.Contains(val, "8.0") {
		t.Errorf("@@version=%s: does not contain '8.0')", val)
	}

	// Make should call the modifyDB plugin. We don't do anything here,
	// but it exists in case users need to tweak the *sql.DB.
	if !called {
		t.Errorf("modifDB callback was not called, expected Make to call it")
	}
}

func TestMyCnf(t *testing.T) {
	// Test that a my.cnf file (config.mysql.mycnf) provides defaults that are
	// not overwritten by explicit values
	f := dbconn.NewConnFactory(nil, nil)

	// Minimal config: username, password, and address with the special test port
	cfg := blip.ConfigMonitor{
		MyCnf: "../test/mycnf/rds-ca",
	}

	// Make makes the connection (sql.DB) or returns an error
	db, dsn, err := f.Make(cfg)
	if err != nil {
		t.Error(err)
	}
	if db == nil {
		t.Fatal("got nil *sql.DB, execpted non-nil value (no return error)")
	}
	defer db.Close()

	// Make returns a print-safe DSN: password ("test") replaced with "..."
	expectDSN := fmt.Sprintf("U:...@tcp(H:33560)/")
	if !strings.HasPrefix(dsn, expectDSN) {
		t.Errorf("got DSN '%s', expected prefix '%s'", dsn, expectDSN)
	}
}
