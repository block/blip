// Copyright 2024 Block, Inc.

package repllag

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/heartbeat"
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

// stopReader stops the heartbeat reader, if any, when the test ends.
func stopReader(t *testing.T, cleanup func()) {
	if cleanup != nil {
		t.Cleanup(cleanup)
	}
}

// spyReader is a heartbeat reader that records whether it was stopped.
type spyReader struct {
	heartbeat.Reader
	stopped atomic.Bool
}

func (r *spyReader) Stop() {
	r.stopped.Store(true)
	r.Reader.Stop()
}

// spyOnReader makes c wrap its heartbeat reader in a spyReader.
func spyOnReader(c *Lag) {
	c.newReader = func(args heartbeat.BlipReaderArgs) heartbeat.Reader {
		return &spyReader{Reader: newBlipReader(args)}
	}
}

// requireReaderStopped requires that Prepare made a reader and stopped it.
// Counting goroutines isn't enough: the reader goroutine might not have
// started yet.
func requireReaderStopped(t *testing.T, c *Lag) {
	t.Helper()
	spy, ok := c.lagReader.(*spyReader)
	if !ok {
		t.Fatalf("Prepare didn't make a heartbeat reader: %T", c.lagReader)
	}
	if !spy.stopped.Load() {
		t.Error("Prepare didn't stop the heartbeat reader")
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
	c := NewLag(unreachableDB(t))
	readers := test.NewGoroutines()
	readers.Label()
	cleanup, err := c.Prepare(context.Background(), blipPlan("fast", "medium", "slow"))
	if err != nil {
		t.Fatal(err)
	}
	if got := readers.Wait(test.HeartbeatReaderFunc, 1, test.HeartbeatReaderTimeout); got != 1 {
		t.Fatalf("%d heartbeat readers running after Prepare, expected 1", got)
	}
	if cleanup == nil {
		t.Fatal("Prepare returned nil cleanup func, so the heartbeat reader can't be stopped")
	}
	cleanup()
	if got := readers.Wait(test.HeartbeatReaderFunc, 0, test.HeartbeatReaderTimeout); got != 0 {
		t.Errorf("%d heartbeat readers running after cleanup, expected 0", got)
	}
}

func TestPrepareBlipErrorStopsReader(t *testing.T) {
	// The reader starts before the repl check is verified, which fails because
	// the database is unreachable. Prepare must stop the reader it started.
	c := NewLag(unreachableDB(t))
	spyOnReader(c)
	plan := blipPlan("kpi")
	plan.Levels["kpi"].Collect[DOMAIN].Options[OPT_REPL_CHECK] = "read_only"
	readers := test.NewGoroutines()
	readers.Label()
	_, err := c.Prepare(context.Background(), plan)
	if err == nil {
		t.Fatal("Prepare returned nil error, expected repl check error")
	}
	requireReaderStopped(t, c)
	if got := readers.Wait(test.HeartbeatReaderFunc, 0, test.HeartbeatReaderTimeout); got != 0 {
		t.Errorf("%d heartbeat readers running after Prepare error, expected 0", got)
	}
}

func TestCollectPFSReturnsDeadlineExceeded(t *testing.T) {
	// Only the replica check query uses the collect context, so set the
	// repl-check option to run it. The lag query uses context.Background(),
	// so the CMR doesn't stop it.
	db := mock.ContextDB()
	defer db.Close()

	c := NewLag(db)
	c.lagWriterIn["kpi"] = LAG_WRITER_PFS
	c.replCheck = "server_id"

	ctx, cancel := context.WithTimeout(context.Background(), -1) // CMR already reached
	defer cancel()
	_, err := c.Collect(ctx, "kpi")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Collect returned error %v, expected an error wrapping context.DeadlineExceeded", err)
	}
}

// fakeReader is a heartbeat reader that reports lag without a database.
type fakeReader struct {
	lag heartbeat.Lag
}

func (r fakeReader) Start() error                               { return nil }
func (r fakeReader) Stop()                                      {}
func (r fakeReader) Lag(context.Context) (heartbeat.Lag, error) { return r.lag, nil }

func TestPrepareBlipAtMultipleLevelsDropsNoHeartbeat(t *testing.T) {
	// report-no-heartbeat defaults to no (drop), and that applies to every
	// level, not only to the level that started the reader. The reader can't
	// reach the database, so it reports no heartbeat.
	for _, report := range []string{"", "no", "yes"} {
		t.Run("report-no-heartbeat="+report, func(t *testing.T) {
			c := NewLag(unreachableDB(t))
			plan := blipPlan("fast", "medium", "slow")
			for _, level := range plan.Levels {
				if report != "" {
					level.Collect[DOMAIN].Options[OPT_REPORT_NO_HEARTBEAT] = report
				}
			}
			readers := test.NewGoroutines()
			readers.Label()
			cleanup, err := c.Prepare(context.Background(), plan)
			stopReader(t, cleanup) // backstop if the test fails before it stops the reader
			if err != nil {
				t.Fatal(err)
			}
			if got := readers.Wait(test.HeartbeatReaderFunc, 1, test.HeartbeatReaderTimeout); got != 1 {
				t.Fatalf("%d heartbeat readers running after Prepare, expected 1 for all levels", got)
			}
			for level := range plan.Levels {
				metrics, err := c.Collect(context.Background(), level)
				if err != nil {
					t.Fatalf("%s: %s", level, err)
				}
				if report == "yes" {
					if len(metrics) != 1 || metrics[0].Value != -1 {
						t.Errorf("%s: got %+v, expected repl.lag.current = -1", level, metrics)
					}
				} else if len(metrics) != 0 {
					t.Errorf("%s: got %+v, expected no metrics (dropped)", level, metrics)
				}
			}
			cleanup() // stopReader calls it again, which is a no-op
			if got := readers.Wait(test.HeartbeatReaderFunc, 0, test.HeartbeatReaderTimeout); got != 0 {
				t.Errorf("%d heartbeat readers running after cleanup, expected 0", got)
			}
		})
	}
}

func TestPrepareBlipPassesReplCheckToReader(t *testing.T) {
	// The reader selects @@<repl-check> to report whether the instance is a
	// replica, so it must be made with the repl-check option. The repl check
	// is verified after the reader starts, and that fails because the database
	// is unreachable, but by then the reader has been made.
	c := NewLag(unreachableDB(t))
	var args []heartbeat.BlipReaderArgs
	c.newReader = func(a heartbeat.BlipReaderArgs) heartbeat.Reader {
		args = append(args, a)
		return fakeReader{}
	}
	plan := blipPlan("fast", "slow")
	for _, level := range plan.Levels {
		level.Collect[DOMAIN].Options[OPT_REPL_CHECK] = "read_only"
	}
	if _, err := c.Prepare(context.Background(), plan); err == nil {
		t.Fatal("Prepare returned nil error, expected repl check error")
	}
	if len(args) != 1 {
		t.Fatalf("Prepare made %d heartbeat readers, expected 1", len(args))
	}
	assert.Equal(t, "read_only", args[0].ReplCheck)
}

func TestPrepareWritersAtDifferentLevels(t *testing.T) {
	// Levels that don't set the writer, or set auto, use the writer set at
	// another level. Plans have them because levels inherit options only from
	// some lower levels. But pfs at one level and blip at another is an error.
	// Each pair is tested in both orders, and many times because Prepare
	// ranges over a map.
	tests := []struct {
		fast, slow string
		writer     string // expected at every level, or "" if Prepare fails
		err        string // expected error, if writer is ""
	}{
		{fast: "", slow: LAG_WRITER_BLIP, writer: LAG_WRITER_BLIP},
		{fast: LAG_WRITER_BLIP, slow: "", writer: LAG_WRITER_BLIP},
		{fast: "auto", slow: LAG_WRITER_BLIP, writer: LAG_WRITER_BLIP},
		{fast: LAG_WRITER_BLIP, slow: "auto", writer: LAG_WRITER_BLIP},
		{fast: LAG_WRITER_BLIP, slow: LAG_WRITER_BLIP, writer: LAG_WRITER_BLIP},
		{fast: "", slow: "auto", writer: LAG_WRITER_BLIP}, // auto-detected: PFS fails because database is unreachable
		{fast: LAG_WRITER_PFS, slow: LAG_WRITER_BLIP, err: "different writer configuration"},
		{fast: LAG_WRITER_BLIP, slow: LAG_WRITER_PFS, err: "different writer configuration"},
		// pfs is set up, not auto-detected or blip, so it fails because the
		// database is unreachable. See TestPrepareAutoAndPFSOnMySQL80.
		{fast: "auto", slow: LAG_WRITER_PFS, err: "could not check replication lag"},
		{fast: LAG_WRITER_PFS, slow: "", err: "could not check replication lag"},
	}
	for _, tt := range tests {
		t.Run(tt.fast+"/"+tt.slow, func(t *testing.T) {
			for i := 0; i < 20; i++ {
				c := NewLag(unreachableDB(t))
				readers := 0
				c.newReader = func(heartbeat.BlipReaderArgs) heartbeat.Reader {
					readers++
					return fakeReader{}
				}
				plan := blipPlan("fast", "slow")
				plan.Levels["fast"].Collect[DOMAIN].Options[OPT_WRITER] = tt.fast
				plan.Levels["slow"].Collect[DOMAIN].Options[OPT_WRITER] = tt.slow
				_, err := c.Prepare(context.Background(), plan)
				if tt.writer == "" {
					if err == nil || !strings.Contains(err.Error(), tt.err) {
						t.Fatalf("run %d: got error %v, expected %q", i, err, tt.err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("run %d: %s", i, err)
				}
				if readers != 1 {
					t.Fatalf("run %d: Prepare made %d heartbeat readers, expected 1", i, readers)
				}
				want := map[string]string{"fast": tt.writer, "slow": tt.writer}
				if !assert.Equal(t, want, c.lagWriterIn, "run %d", i) {
					return
				}
			}
		})
	}
}

func TestPrepareSetsOptionsAtEveryLevel(t *testing.T) {
	// The writer is set up once, but every level has its own report and
	// channel name options.
	c := NewLag(unreachableDB(t))
	c.newReader = func(heartbeat.BlipReaderArgs) heartbeat.Reader {
		return fakeReader{lag: heartbeat.Lag{Replica: false, Milliseconds: -1}}
	}
	plan := blipPlan("fast", "medium", "slow")
	plan.Levels["fast"].Collect[DOMAIN].Options[OPT_REPORT_NO_HEARTBEAT] = "yes"
	plan.Levels["medium"].Collect[DOMAIN].Options[OPT_REPORT_NOT_A_REPLICA] = "yes"
	plan.Levels["slow"].Collect[DOMAIN].Options[OPT_DEFAULT_CHANNEL_NAME] = "main"
	if _, err := c.Prepare(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, map[string]string{"fast": LAG_WRITER_BLIP, "medium": LAG_WRITER_BLIP, "slow": LAG_WRITER_BLIP}, c.lagWriterIn)
	assert.Equal(t, map[string]bool{"fast": false, "medium": true, "slow": true}, c.dropNoHeartbeat)
	assert.Equal(t, map[string]bool{"fast": true, "medium": false, "slow": true}, c.dropNotAReplica)
	assert.Equal(t, map[string]string{"fast": "", "medium": "", "slow": "main"}, c.defaultChannelNameOverrides)

	// Not a replica: reported only at the level with report-not-a-replica=yes
	for level := range plan.Levels {
		metrics, err := c.Collect(context.Background(), level)
		if err != nil {
			t.Fatalf("%s: %s", level, err)
		}
		if level == "medium" {
			assert.Len(t, metrics, 1, level)
		} else {
			assert.Empty(t, metrics, level)
		}
	}
}

func TestPrepareAutoAndPFSOnMySQL80(t *testing.T) {
	_, db, err := test.Connection("mysql80")
	if err != nil {
		if test.Build {
			t.Skip("mysql80 not running")
		} else {
			t.Fatal(err)
		}
	}
	defer db.Close()

	// Same as auto and blip in TestPrepareWritersAtDifferentLevels, but pfs
	// needs MySQL
	for _, writers := range [][2]string{{"auto", LAG_WRITER_PFS}, {LAG_WRITER_PFS, ""}} {
		for i := 0; i < 20; i++ {
			c := NewLag(db)
			plan := blipPlan("fast", "slow")
			plan.Levels["fast"].Collect[DOMAIN].Options[OPT_WRITER] = writers[0]
			plan.Levels["slow"].Collect[DOMAIN].Options[OPT_WRITER] = writers[1]
			cleanup, err := c.Prepare(context.Background(), plan)
			stopReader(t, cleanup)
			if err != nil {
				t.Fatalf("%v run %d: %s", writers, i, err)
			}
			if c.lagReader != nil {
				t.Fatalf("%v run %d: Prepare made a heartbeat reader for pfs", writers, i)
			}
			assert.Equal(t, map[string]string{"fast": LAG_WRITER_PFS, "slow": LAG_WRITER_PFS}, c.lagWriterIn)
		}
	}
}
