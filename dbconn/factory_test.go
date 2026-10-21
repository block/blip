// Copyright 2024 Block, Inc.

package dbconn_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

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
	// The default connect timeout (10s) is applied even though cfg.TimeoutConnect is empty.
	expectDSN := fmt.Sprintf("%s:...@tcp(%s)/?interpolateParams=true&parseTime=true&timeout=10s", cfg.Username, cfg.Hostname)
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

func TestConnectTimeoutDSN(t *testing.T) {
	// config.monitor.timeout-connect must become the driver DSN "timeout" param,
	// else the driver dials with no timeout. Make doesn't connect to MySQL, so
	// these don't need a running MySQL instance.
	f := dbconn.NewConnFactory(nil, nil)

	tests := []struct {
		timeoutConnect string
		expectParam    string // empty: no timeout param
	}{
		{"", "timeout=10s"}, // default when not set
		{"3s", "timeout=3s"},
		{"1500ms", "timeout=1.5s"},
		{"1m", "timeout=1m0s"},
		{"0", ""},  // explicitly disabled
		{"0s", ""}, // explicitly disabled
	}
	for _, tt := range tests {
		t.Run(tt.timeoutConnect, func(t *testing.T) {
			cfg := blip.ConfigMonitor{
				MonitorId:      "m1",
				Username:       "root",
				Password:       "test",
				Hostname:       "127.0.0.1:33999",
				TimeoutConnect: tt.timeoutConnect,
			}
			db, dsn, err := f.Make(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if tt.expectParam == "" {
				if strings.Contains(dsn, "timeout=") {
					t.Errorf("got DSN '%s', expected no timeout param", dsn)
				}
				return
			}
			expectDSN := "root:...@tcp(127.0.0.1:33999)/?interpolateParams=true&parseTime=true&" + tt.expectParam
			if dsn != expectDSN {
				t.Errorf("got DSN '%s', expected '%s'", dsn, expectDSN)
			}
		})
	}

	// Invalid values are config errors, not silently ignored
	for _, bad := range []string{"10", "ten seconds", "-1s"} {
		cfg := blip.ConfigMonitor{
			MonitorId:      "m1",
			Hostname:       "127.0.0.1:33999",
			TimeoutConnect: bad,
		}
		db, _, err := f.Make(cfg)
		if err == nil {
			db.Close()
			t.Errorf("timeout-connect=%q: no error, expected one", bad)
		}
	}
}

func TestConnectTimeoutReloadDSN(t *testing.T) {
	// When credentials are reloaded, the hotswap driver gets a new DSN from
	// Repo.ReloadDSN. It must keep the connect timeout.
	addr := "127.0.0.1:33998"
	dbconn.Repo.Add(addr, func(context.Context) (blip.DbCredentials, error) {
		return blip.DbCredentials{Username: "root", Password: "new"}, nil
	})
	newDSN := dbconn.Repo.ReloadDSN(context.Background(), "root:old@tcp("+addr+")/?parseTime=true&timeout=3s")
	if !strings.Contains(newDSN, "root:new@") {
		t.Fatalf("got DSN '%s', expected new credentials", newDSN)
	}
	if !strings.Contains(newDSN, "timeout=3s") {
		t.Errorf("got DSN '%s', expected timeout=3s", dbconn.RedactedDSN(newDSN))
	}
}

func TestConnectTimeoutBoundsDial(t *testing.T) {
	// Connect to a non-routable address. Packets to it are dropped, so without
	// a dial timeout the connect blocks until the OS gives up on the TCP SYN
	// (about 75s on macOS, about 130s on Linux). With timeout-connect=1s, it
	// must fail in about 1s. If the network rejects the address immediately
	// (e.g. no route to host), the test still passes: the dial is bounded.
	if testing.Short() {
		t.Skip("dials a non-routable address")
	}
	f := dbconn.NewConnFactory(nil, nil)
	cfg := blip.ConfigMonitor{
		MonitorId:      "m1",
		Username:       "root",
		Password:       "test",
		Hostname:       "10.255.255.1:3306",
		TimeoutConnect: "1s",
	}
	db, _, err := f.Make(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// The context deadline is a backstop so a regression fails instead of
	// hanging; it's much longer than timeout-connect to prove the DSN timeout,
	// not the context, ends the dial.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	t0 := time.Now()
	err = db.PingContext(ctx)
	elapsed := time.Since(t0)
	if err == nil {
		t.Fatal("ping to non-routable address succeeded, expected error")
	}
	t.Logf("ping error after %s: %s", elapsed, err)
	if ctx.Err() != nil {
		t.Fatalf("ping ended by context deadline after %s, expected dial timeout of 1s: %s", elapsed, err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("ping took %s, expected about 1s (timeout-connect)", elapsed)
	}
}
