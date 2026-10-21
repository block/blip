// Copyright 2024 Block, Inc.

package repllag

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/test"
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
	_, err = c.Prepare(context.Background(), defaultPlan)
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
	_, err = c.Prepare(context.Background(), plan)
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
	_, err = c.Prepare(context.Background(), plan)
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
		_, err = c.Prepare(context.Background(), plan)
		if err != nil {
			t.Error(err)
		}
		metrics, _ := c.Collect(context.TODO(), "kpi")
		assert.Equal(t, 0, len(metrics))
	}
}

const readerFunc = "heartbeat.(*BlipReader).run"

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
	before := test.Goroutines(readerFunc)

	c := NewLag(unreachableDB(t))
	cleanup, err := c.Prepare(context.Background(), blipPlan("fast", "medium", "slow"))
	if err != nil {
		t.Fatal(err)
	}
	if got := test.WaitGoroutines(readerFunc, before+1, time.Second); got != before+1 {
		t.Fatalf("%d heartbeat readers running after Prepare, expected %d", got, before+1)
	}
	if cleanup == nil {
		t.Fatal("Prepare returned nil cleanup func, so the heartbeat reader can't be stopped")
	}
	cleanup()
	if got := test.WaitGoroutines(readerFunc, before, time.Second); got != before {
		t.Errorf("%d heartbeat readers running after cleanup, expected %d", got, before)
	}
}

func TestPrepareBlipErrorStopsReader(t *testing.T) {
	// The reader starts before the repl check is verified, which fails because
	// the database is unreachable. Prepare must stop the reader it started.
	before := test.Goroutines(readerFunc)

	c := NewLag(unreachableDB(t))
	plan := blipPlan("kpi")
	plan.Levels["kpi"].Collect[DOMAIN].Options[OPT_REPL_CHECK] = "read_only"
	if _, err := c.Prepare(context.Background(), plan); err == nil {
		t.Fatal("Prepare returned nil error, expected repl check error")
	}
	if got := test.WaitGoroutines(readerFunc, before, time.Second); got != before {
		t.Errorf("%d heartbeat readers running after Prepare error, expected %d", got, before)
	}
}
