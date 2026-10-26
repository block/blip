// Copyright 2026 Block, Inc.

package heartbeat_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/heartbeat"
	"github.com/cashapp/blip/v2/status"
)

// writeTestDriver is a fake SQL driver for the heartbeat writer: every write
// succeeds, or fails with a MySQL read-only error if writeTestReadOnly is set.
const writeTestDriver = "blip-heartbeat-write-test"

var writeTestReadOnly atomic.Bool

func init() {
	sql.Register(writeTestDriver, writeTestSQLDriver{})
}

type writeTestSQLDriver struct{}

func (writeTestSQLDriver) Open(string) (driver.Conn, error) { return writeTestConn{}, nil }

type writeTestConn struct{}

func (writeTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("no prepared statements")
}
func (writeTestConn) Close() error              { return nil }
func (writeTestConn) Begin() (driver.Tx, error) { return nil, errors.New("no transactions") }
func (writeTestConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	if writeTestReadOnly.Load() {
		return nil, &mysql.MySQLError{Number: 1290, Message: "read-only"}
	}
	return driver.RowsAffected(1), nil
}

// stopWriter starts w, waits until its status contains want, then stops it.
// It returns how long Write took to return after stop.
func stopWriter(t *testing.T, w *heartbeat.Writer, monitorId, want string) time.Duration {
	t.Helper()
	stopChan := make(chan struct{})
	doneChan := make(chan struct{})
	go w.Write(stopChan, doneChan)

	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(status.ReportMonitors(monitorId)[monitorId][status.HEARTBEAT_WRITER], want) {
		if time.Now().After(deadline) {
			close(stopChan)
			t.Fatalf("timeout waiting for heartbeat writer status %q", want)
		}
		time.Sleep(5 * time.Millisecond)
	}

	t0 := time.Now()
	close(stopChan)
	select {
	case <-doneChan:
	case <-time.After(time.Minute):
		t.Fatal("heartbeat writer did not stop")
	}
	return time.Since(t0)
}

func TestWriterStopsDuringWaits(t *testing.T) {
	// Stopping the writer must not wait for its sleep: the heartbeat
	// frequency, or the read-only wait (20s by default).
	const monitorId = "hb-write-stop"
	defer status.RemoveMonitor(monitorId)
	db, err := sql.Open(writeTestDriver, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const wait = 30 * time.Second
	defer func(d time.Duration) { heartbeat.ReadOnlyWait = d }(heartbeat.ReadOnlyWait)
	heartbeat.ReadOnlyWait = wait

	// Read-only: the first write fails, and the writer sleeps ReadOnlyWait
	writeTestReadOnly.Store(true)
	defer writeTestReadOnly.Store(false)
	w := heartbeat.NewWriter(monitorId, db, blip.ConfigHeartbeat{Freq: "1s", Table: "heartbeat"})
	if d := stopWriter(t, w, monitorId, "read-only"); d > wait/10 {
		t.Errorf("stop during read-only wait took %s, expected well under %s", d, wait)
	}

	// Writable: the writer sleeps the heartbeat frequency between writes
	writeTestReadOnly.Store(false)
	w = heartbeat.NewWriter(monitorId, db, blip.ConfigHeartbeat{Freq: wait.String(), Table: "heartbeat"})
	if d := stopWriter(t, w, monitorId, "sleep"); d > wait/10 {
		t.Errorf("stop during heartbeat sleep took %s, expected well under %s", d, wait)
	}
}
