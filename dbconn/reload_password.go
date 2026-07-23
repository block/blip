// Copyright 2024 Block, Inc.

package dbconn

import (
	"context"
	"sync"

	dsndriver "github.com/go-mysql/hotswap-dsn-driver"
	"github.com/go-sql-driver/mysql"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/credentials"
	"github.com/cashapp/blip/v2/event"
)

func init() {
	dsndriver.SetHotswapFunc(Repo.ReloadDSN)
}

// CredentialFunc is retained as an alias for compatibility with integrations
// that used the MySQL dbconn package to construct credential callbacks.
type CredentialFunc = credentials.Func

type repo struct {
	m *sync.Map
}

var Repo = &repo{
	m: &sync.Map{},
}

func (r *repo) Add(addr string, f CredentialFunc) error {
	r.m.Store(addr, f)
	blip.Debug("added %s", addr)
	return nil
}

func (r *repo) ReloadDSN(ctx context.Context, currentDSN string) string {
	// Only return new DSN on success and credentials are different. Else, return
	// an empty string which makes the hotswap driver return the original driver
	// error, i.e. it's like this func was never called. Only when this func
	// returns a non-empty string does the hotswap driver use it to swap out
	// the low-level MySQL connection.
	blip.Debug("reloading %s", RedactedDSN(currentDSN))

	cfg, err := mysql.ParseDSN(currentDSN)
	if err != nil {
		blip.Debug("error parsing DSN %s: %s", currentDSN, err)
		return ""
	}

	v, ok := r.m.Load(cfg.Addr)
	if !ok {
		blip.Debug("no credential func for %s", cfg.Addr)
		return ""
	}

	newCred, err := v.(CredentialFunc)(ctx)
	if err != nil {
		event.Sendf(event.DB_RELOAD_PASSWORD_ERROR, "%s: %s", RedactedDSN(currentDSN), err.Error())
		return ""
	}

	if cfg.Passwd == newCred.Password && cfg.User == newCred.Username {
		blip.Debug("credentials have not changed")
		return ""
	}

	cfg.Passwd = newCred.Password
	cfg.User = newCred.Username
	newDSN := cfg.FormatDSN()
	blip.Debug("credentials reloaded; new DSN: %s", RedactedDSN(newDSN))
	return newDSN
}
