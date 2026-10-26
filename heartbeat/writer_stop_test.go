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
// succeeds, or fails with a MySQL read-only error if writeTestReadOnly is set,
// or blocks until its context is canceled if writeTestHung is set.
const writeTestDriver = "blip-heartbeat-write-test"

var (
	writeTestReadOnly atomic.Bool
	writeTestHung     atomic.Bool
	writeTestHungExec = make(chan struct{}, 10) // a hung write has started
)

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
func (writeTestConn) ExecContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	if writeTestHung.Load() {
		writeTestHungExec <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
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

func TestWriterStopsDuringWrite(t *testing.T) {
	// A write can wait up to WriteTimeout on a hung or partitioned MySQL.
	// Stopping the writer cancels it instead of waiting.
	const monitorId = "hb-write-stop-hung"
	defer status.RemoveMonitor(monitorId)
	db, err := sql.Open(writeTestDriver, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const timeout = 30 * time.Second
	defer func(d time.Duration) { heartbeat.WriteTimeout = d }(heartbeat.WriteTimeout)
	heartbeat.WriteTimeout = timeout

	writeTestHung.Store(true)
	defer writeTestHung.Store(false)

	// First write (INSERT) hangs. The status is set before the write, so the
	// stop can come just before the write starts, which tests stopping it too.
	w := heartbeat.NewWriter(monitorId, db, blip.ConfigHeartbeat{Freq: "1s", Table: "heartbeat"})
	if d := stopWriter(t, w, monitorId, "first insert"); d > timeout/10 {
		t.Errorf("stop during first write took %s, expected well under %s", d, timeout)
	}
	for len(writeTestHungExec) > 0 {
		<-writeTestHungExec
	}

	// A heartbeat (UPDATE) hangs
	writeTestHung.Store(false)
	w = heartbeat.NewWriter(monitorId, db, blip.ConfigHeartbeat{Freq: "10ms", Table: "heartbeat"})
	stopChan := make(chan struct{})
	doneChan := make(chan struct{})
	go w.Write(stopChan, doneChan)
	deadline := time.Now().Add(5 * time.Second)
	for status.ReportMonitors(monitorId)[monitorId][status.HEARTBEAT_WRITER] != "sleep" {
		if time.Now().After(deadline) {
			close(stopChan)
			t.Fatal("timeout waiting for first write")
		}
		time.Sleep(time.Millisecond)
	}
	writeTestHung.Store(true)
	select {
	case <-writeTestHungExec:
	case <-time.After(5 * time.Second):
		close(stopChan)
		t.Fatal("timeout waiting for hung heartbeat")
	}
	t0 := time.Now()
	close(stopChan)
	select {
	case <-doneChan:
	case <-time.After(2 * timeout):
		t.Fatal("heartbeat writer did not stop")
	}
	if d := time.Since(t0); d > timeout/10 {
		t.Errorf("stop during heartbeat write took %s, expected well under %s", d, timeout)
	}
}
