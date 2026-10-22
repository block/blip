// Copyright 2024 Block, Inc.

package repllag

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/test"
	"github.com/cashapp/blip/v2/test/mock"
)

func TestPrepareForSingleLevelAndNoSourceOnMySQL80(t *testing.T) {
	// default source on MySQL 8.0 should be `pfs`
	_, db, err := test.Connection("mysql80")
	if err != nil {
		if test.Build {
			t.Skip("mysql80 not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	c := NewLag(db)

	defaultPlan := test.ReadPlan(t, "")
	cleanup, err := c.Prepare(context.Background(), defaultPlan)
	stopReader(t, cleanup)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, "pfs", c.lagWriterIn["kpi"])
}

func TestPrepareWithInvalidSource(t *testing.T) {
	_, db, err := test.Connection("mysql80")
	if err != nil {
		if test.Build {
			t.Skip("mysql80 not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	c := NewLag(db)

	plan := test.ReadPlan(t, "")
	plan.Levels["kpi"].Collect[DOMAIN].Options[OPT_WRITER] = "invalid-lag-source"
	cleanup, err := c.Prepare(context.Background(), plan)
	stopReader(t, cleanup)
	assert.Error(t, err)
}

func TestCollectWithNoSource(t *testing.T) {
	// defaults to pfs
	_, db, err := test.Connection("mysql80")
	if err != nil {
		if test.Build {
			t.Skip("mysql80 not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	c := NewLag(db)

	plan := test.ReadPlan(t, "")
	plan.Levels["kpi"].Collect[DOMAIN].Options[OPT_REPORT_NOT_A_REPLICA] = "yes"
	plan.Levels["kpi"].Collect[DOMAIN].Options[OPT_REPL_CHECK] = "read_only"
	cleanup, err := c.Prepare(context.Background(), plan)
	stopReader(t, cleanup)
	if err != nil {
		t.Error(err)
	}
	metrics, err := c.Collect(context.TODO(), "kpi")
	assert.Equal(t, 1, len(metrics))
	assert.NoError(t, err)
}

func TestCollectWithAllSources(t *testing.T) {
	// defaults to pfs
	_, db, err := test.Connection("mysql80")
	if err != nil {
		if test.Build {
			t.Skip("mysql80 not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	srcs := [3]string{"auto", "pfs", "blip"}

	for _, src := range srcs {
		c := NewLag(db)
		plan := test.ReadPlan(t, "")
		plan.Levels["kpi"].Collect[DOMAIN].Options[OPT_REPORT_NOT_A_REPLICA] = "no"
		if src == "blip" {
			plan.Levels["kpi"].Collect[DOMAIN].Options[OPT_REPORT_NO_HEARTBEAT] = "no"
		}
		plan.Levels["kpi"].Collect[DOMAIN].Options[OPT_WRITER] = src
		cleanup, err := c.Prepare(context.Background(), plan)
		stopReader(t, cleanup)
		if err != nil {
			t.Error(err)
		}
		metrics, _ := c.Collect(context.TODO(), "kpi")
		assert.Equal(t, 0, len(metrics))
	}
}

const (
	readerFunc    = "heartbeat.(*BlipReader).run"
	readerTimeout = 5 * time.Second // > heartbeat.ReadTimeout: reader exits after an in-flight query
)

// stopReader stops the heartbeat reader, if any, when the test ends. Tests
// that count reader goroutines require that earlier tests don't leak them.
func stopReader(t *testing.T, cleanup func()) {
	if cleanup != nil {
		t.Cleanup(cleanup)
	}
}

// requireNoReaders waits for readers stopped by earlier tests to exit, so the
// test can count only the readers that it starts.
func requireNoReaders(t *testing.T) {
	t.Helper()
	if got := test.WaitGoroutines(readerFunc, 0, readerTimeout); got != 0 {
		t.Fatalf("%d heartbeat readers running before test, expected 0 (leaked by an earlier test?)", got)
	}
}

// unreachableDB returns a handle that fails every query. Preparing the blip
// writer doesn't query the database, and the reader just retries its query.
func unreachableDB(t *testing.T) *sql.DB {
	db, err := sql.Open("mysql", "root:test@tcp(127.0.0.1:1)/?timeout=100ms")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func blipPlan(levels ...string) blip.Plan {
	plan := blip.Plan{Name: "lag-cleanup", MonitorId: "lag-cleanup", Levels: map[string]blip.Level{}}
	for _, level := range levels {
		plan.Levels[level] = blip.Level{
			Name: level,
			Freq: "5s",
			Collect: map[string]blip.Domain{
				DOMAIN: {Name: DOMAIN, Options: map[string]string{OPT_WRITER: LAG_WRITER_BLIP}},
			},
		}
	}
	return plan
}

func TestPrepareBlipAtMultipleLevelsReturnsReaderCleanup(t *testing.T) {
	// Plans usually collect repl.lag at more than one level because levels
	// inherit domains from lower levels. Prepare starts one reader for all
	// levels, so it must return the cleanup func that stops that reader.
	requireNoReaders(t)

	c := NewLag(unreachableDB(t))
	cleanup, err := c.Prepare(context.Background(), blipPlan("fast", "medium", "slow"))
	if err != nil {
		t.Fatal(err)
	}
	if got := test.WaitGoroutines(readerFunc, 1, readerTimeout); got != 1 {
		t.Fatalf("%d heartbeat readers running after Prepare, expected 1", got)
	}
	if cleanup == nil {
		t.Fatal("Prepare returned nil cleanup func, so the heartbeat reader can't be stopped")
	}
	cleanup()
	if got := test.WaitGoroutines(readerFunc, 0, readerTimeout); got != 0 {
		t.Errorf("%d heartbeat readers running after cleanup, expected 0", got)
	}
}

func TestPrepareBlipErrorStopsReader(t *testing.T) {
	// The reader starts before the repl check is verified, which fails because
	// the database is unreachable. Prepare must stop the reader it started.
	requireNoReaders(t)

	c := NewLag(unreachableDB(t))
	plan := blipPlan("kpi")
	plan.Levels["kpi"].Collect[DOMAIN].Options[OPT_REPL_CHECK] = "read_only"
	if _, err := c.Prepare(context.Background(), plan); err == nil {
		t.Fatal("Prepare returned nil error, expected repl check error")
	}
	if got := test.WaitGoroutines(readerFunc, 0, readerTimeout); got != 0 {
		t.Errorf("%d heartbeat readers running after Prepare error, expected 0", got)
	}
}

func TestCollectPFSWrapsQueryError(t *testing.T) {
	// One case per query in collectPFS: the replica check query (only run
	// when the repl-check option is set) and the lag query
	for _, replCheck := range []string{"server_id", ""} {
		t.Run("replCheck="+replCheck, func(t *testing.T) {
			db := mock.ErrorDB(fmt.Errorf("query: %w", context.DeadlineExceeded))
			defer db.Close()

			c := NewLag(db)
			c.lagWriterIn["kpi"] = LAG_WRITER_PFS
			c.replCheck = replCheck

			_, err := c.Collect(context.Background(), "kpi")
			if err == nil {
				t.Fatal("Collect returned nil error, expected query error")
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("errors.Is(err, context.DeadlineExceeded) = false, expected true; err: %v", err)
			}
		})
	}
}
