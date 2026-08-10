// Copyright 2024 Block, Inc.

package repllag

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

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
