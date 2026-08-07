// Copyright 2024 Block, Inc.

package heartbeat_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/heartbeat"
	"github.com/cashapp/blip/v2/test"
	"github.com/cashapp/blip/v2/test/mock"
)

const (
	blip_writer_db    = "blip_test"
	blip_writer_table = blip_writer_db + ".heartbeat"
)

func setupHeartbeatTable(db *sql.DB) error {
	// Before each test, totally recreate the heartbeat db and table so each
	// test begins with a clean slate (an empty table)
	queries := []string{
		"DROP DATABASE IF EXISTS " + blip_writer_db,
		"CREATE DATABASE IF NOT EXISTS " + blip_writer_db,
		"USE " + blip_writer_db,
		heartbeat.BLIP_TABLE_DDL,
	}
	for _, q := range queries {
		_, err := db.Exec(q)
		if err != nil {
			return err
		}
	}
	return nil
}

// hbRows is one row from the heartbeat table, which has three columns
type hbRows struct {
	monitorId string
	ts        time.Time
	freq      uint
}

func heartbreatRows(db *sql.DB) ([]hbRows, error) {
	rows, err := db.Query("SELECT src_id, ts, freq FROM " + blip_writer_table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	hbrows := []hbRows{}
	for rows.Next() {
		r := hbRows{}
		if err := rows.Scan(&r.monitorId, &r.ts, &r.freq); err != nil {
			return nil, err
		}
		hbrows = append(hbrows, r)
	}
	return hbrows, nil
}

// --------------------------------------------------------------------------

func TestWriter(t *testing.T) {
	_, db, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip(test.DefaultMySQLVersion + " not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	// Test that the Blip heartbeat writer writes heatbeat rows to its table,
	// a.k.a "it works"
	if err := setupHeartbeatTable(db); err != nil {
		t.Fatal(err)
	}

	// A new writer requires these 2 config values, else it panics
	cfg := blip.ConfigHeartbeat{
		Freq:  "100ms",
		Table: blip_writer_table,
	}
	wr := heartbeat.NewWriter("m1", db, cfg)

	// But before running the writer, verify that we're starting with a clean
	// slate: zero heartbeat rows in the table. If not, the tests might not
	// be cleaning up before they run, or they're running in parallel (which
	// they're not current designed to do)
	gotRows, err := heartbreatRows(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotRows) != 0 {
		t.Fatalf("got %d heartbeat rows before running Writer, expected 0: %v", len(gotRows), gotRows)
	}

	// Run the writer goroutine and give it 200ms to work, which should be enough
	// given the 100ms freq configured
	stopChan := make(chan struct{})
	doneChan := make(chan struct{})
	go wr.Write(stopChan, doneChan)

	time.Sleep(200 * time.Millisecond)

	// Stop the writer goroutine
	close(stopChan)
	select {
	case <-doneChan:
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for BlipWriter.Write goroutine to stop")
	}

	// Wouldn't you know: there can be a few milliseconds of local clock skew.
	// This avoids a false-positive test failure like:
	//   blip_writer_test.go:106: heartbeat ts in the future: 2021-12-10 17:06:31.336 +0000 UTC
	//                                                   (now=2021-12-10 17:06:31.326124 +0000 UTC)
	// Or it's not clock skew, just rounding weirdness.
	time.Sleep(100 * time.Millisecond)

	// Get all heartbeat rows; there should only be 1
	gotRows, err = heartbreatRows(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotRows) != 1 {
		t.Fatalf("got %d heartbeat rows before running Writer, expected 1: %v", len(gotRows), gotRows)
	}

	// Heartbeat row monitor ID should = "m1" from above
	if gotRows[0].monitorId != "m1" {
		t.Errorf("heartbeat row src_id=%s, expected m1", gotRows[0].monitorId)
	}

	// Heartbeat row can't be in the future; Blip is advanced, but not that advanced
	now := time.Now().UTC()
	if gotRows[0].ts.After(now) {
		t.Errorf("heartbeat ts in the future: %s (now=%s)", gotRows[0].ts, now)
	}

	// Heartbeat row should't be too far in past, else it might not be a row
	// that this test just wrote
	elapsed := time.Now().Sub(gotRows[0].ts).Seconds()
	if elapsed > 1.0 {
		t.Errorf("heartbeat ts older than 1.0 second: %s (now=%s)", gotRows[0].ts, now)
	}
}

func TestReader(t *testing.T) {
	_, db, err := test.Connection(test.DefaultMySQLVersion)
	if err != nil {
		if test.Build {
			t.Skip(test.DefaultMySQLVersion + " not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	heartbeat.ReadErrorWait = 500 * time.Millisecond
	defer func() { heartbeat.ReadErrorWait = 1 * time.Second }()

	// Create heartbeat table and make sure it has zero rows to start
	if err := setupHeartbeatTable(db); err != nil {
		t.Fatal(err)
	}
	gotRows, err := heartbreatRows(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotRows) != 0 {
		t.Fatalf("got %d heartbeat rows before running Writer, expected 0: %v", len(gotRows), gotRows)
	}

	// Now simulate a writer by writing a heatbeat row. The important
	// part is 200 (milliseconds): the reader will expect the next heartbeat
	// in NOW + 200ms, which is easy to do in this by updating the heartbeat
	// row in 200ms--or, to simulate lag, don't update it, or update it late.
	q := "INSERT INTO " + blip_writer_table + " VALUES ('s1', NULL, NOW(3), 200)"
	if _, err := db.Exec(q); err != nil {
		t.Fatal(err)
	}

	// Create and start reader. To see inside what it's doing while running,
	// the mock LagWaiter wraps the real LagWaiter, which allows us to intercept
	// the real lag (and wait time, which we ignore) that the real waiter
	// returns to the reader.
	hbChan := make(chan int64, 1)
	realWaiter := heartbeat.SlowFastWaiter{NetworkLatency: 10 * time.Millisecond}
	mockWaiter := mock.LagWaiter{
		WaitFunc: func(now, then time.Time, f int, srcId string) (int64, time.Duration) {
			lag, wait := realWaiter.Wait(now, then, f, srcId)
			hbChan <- lag
			return lag, wait
		},
	}
	hr := heartbeat.NewBlipReader(heartbeat.BlipReaderArgs{
		MonitorId: "r1",
		DB:        db,
		Table:     blip_writer_table,
		Waiter:    mockWaiter,
	})
	hr.Start()

	timeout := time.After(5 * time.Second) // this whole test should take <1s

	// First read: heartbeat says next one in 200 ms
	var lag1, lag2 int64
	select {
	case lag1 = <-hbChan:
	case <-timeout:
		t.Fatal("timeout waiting for LagWaiter")
	}

	// No lag yet because reader just started
	if lag1 != 0 {
		t.Errorf("lag = %d ms, expected at start", lag1)
	}

	// Second read: should be slightly more than 200 ms after first because
	// reader expect next heartbeat in 200 ms + some lag, so it waits 200 + N ms
	// where N is currently 50 ms to start.
	t0 := time.Now()
	select {
	case lag1 = <-hbChan:
	case <-timeout:
		t.Fatal("timeout waiting for LagWaiter")
	}
	waited := time.Now().Sub(t0)
	waitedMs := waited.Milliseconds()
	if waitedMs < 200 || waitedMs > 300 {
		t.Errorf("waited %d ms, expected between 200 and 300 ms", waitedMs)
	}

	// Lag should be reported because we're slightly after the expected ts
	// of the next heartbeat, which this test hasn't written yet
	if lag1 < 10 || lag1 > 50 {
		t.Errorf("lag = %d ms, expected between 10 and 50 ms", lag1)
	}

	// Third read: still lagging, so lag should be greater than before
	select {
	case lag2 = <-hbChan:
	case <-timeout:
		t.Fatal("timeout waiting for LagWaiter")
	}
	if lag2 <= lag1 {
		t.Errorf("lag did not increase: %d ms -> %d ms, expected 2nd to be greater value", lag1, lag2)
	}

	lag1 = lag2 // shift to keep last two

	// Fourth read: still lagging, so lag should be greater than before
	select {
	case lag2 = <-hbChan:
	case <-timeout:
		t.Fatal("timeout waiting for LagWaiter")
	}
	if lag2 <= lag1 {
		t.Errorf("lag did not increase: %d ms -> %d ms, expected 2nd to be greater value", lag1, lag2)
	}

	lag1 = lag2 // shift to keep last two

	// Write 2nd heartbeat and wait for fifth read
	q = "UPDATE " + blip_writer_table + " SET ts=NOW(3) WHERE src_id='s1'"
	if _, err := db.Exec(q); err != nil {
		t.Fatal(err)
	}
	select {
	case lag2 = <-hbChan:
	case <-timeout:
		t.Fatal("timeout waiting for LagWaiter")
	}

	// Now we're current wrt 2nd heartbeat, so lag should be less than before: slighly
	// after 2nd heartbeat. In tests, this typically reports lag2=42ms which is actually
	// more a reflet of the 50ms wait from the 4th read.
	if lag2 > lag1 {
		t.Errorf("lag not reset after 2nd heartbeat: lag2=%d > lag1=%d", lag2, lag1)
	}

	lag1 = lag2 // shift to keep last two

	// Sixth read: now the lag _decreases_ because it's waiting for 2nd heatbeat,
	// and it's just past the expected time, so we know we're slightly lagging
	// again, so the value resets to the new, known lag
	select {
	case lag2 = <-hbChan:
	case <-timeout:
		t.Fatal("timeout waiting for LagWaiter")
	}
	if lag2 > lag1 {
		t.Errorf("lag did not decrease: %d ms -> %d ms, expected 2nd to be lesser value", lag1, lag2)
	}
	if lag2 < 10 || lag2 > 60 {
		t.Errorf("lag = %d ms, expected between 50 and 100 ms", lag2)
	}
}
