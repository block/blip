// Copyright 2024 Block, Inc.

// Package dbconn provides a Factory that makes *sql.DB connections to MySQL.
package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/cashapp/blip"
	"github.com/cashapp/blip/aws"
)

// rdsAddr matches Amazon RDS hostnames with optional :port suffix.
// It's used to automatically load the Amazon RDS CA and enable TLS,
// unless config.aws.disable-auto-tls is true.
var rdsAddr = regexp.MustCompile(`rds\.amazonaws\.com(:\d+)?$`)

// portSuffix matches optional :port suffix on addresses. It's used to
// strip the port suffix before passing the hostname to LoadTLS.
var portSuffix = regexp.MustCompile(`:\d+$`)

// factory is the internal implementation of blip.DbFactory.
type factory struct {
	awsConfig blip.AWSConfigFactory
	modifyDB  func(*sql.DB, string)
}

// NewConnFactory returns a blip.NewConnFactory that connects to MySQL.
// This is the only blip.NewConnFactor. It is created in Server.Defaults.
func NewConnFactory(awsConfig blip.AWSConfigFactory, modifyDB func(*sql.DB, string)) factory {
	return factory{
		awsConfig: awsConfig,
		modifyDB:  modifyDB,
	}
}

// Make makes a *sql.DB for the given monitor config. On success, it also returns
// a print-safe DSN (with any password replaced by "..."). The config must be
// copmlete: defaults, env var, and monitor var interpolations already applied,
// which is done by the monitor.Loader in its private merge method.
func (f factory) Make(cfg blip.ConfigMonitor) (*sql.DB, string, error) {
	// ----------------------------------------------------------------------
	// my.cnf

	// Make a copy of the config before modifying it so that it can be used later,
	// specifically for reloading TLS where we need to determine where the updates
	// should come from
	originalCfg := cfg

	// Set values in cfg blip.ConfigMonitor from values in my.cnf. This does
	// not overwrite any values in cfg already set. For exmaple, if username
	// is specified in both, the default my.cnf username is ignored and the
	// explicit cfg.Username is kept/used.
	if cfg.MyCnf != "" {
		blip.Debug("%s reads mycnf %s", cfg.MonitorId, cfg.MyCnf)
		def, tls, err := ParseMyCnf(cfg.MyCnf)
		if err != nil {
			return nil, "", err
		}
		cfg.ApplyDefaults(blip.Config{MySQL: def, TLS: tls})
	}

	// ----------------------------------------------------------------------
	// TCP or Unix socket

	net := ""
	addr := ""
	if cfg.Socket != "" {
		net = "unix"
		addr = cfg.Socket
	} else {
		net = "tcp"
		addr = cfg.Hostname
	}

	// ----------------------------------------------------------------------
	// Pasword reload func

	// Blip presumes that credentials are rotated for security. So we create
	// a callback that relaods the credentials based on its method: static, file,
	// Amazon IAM auth token, etc. The special mysql-hotswap-dsn driver (below)
	// calls this func when MySQL returns an authentication error.
	credentialFunc, err := f.Credentials(cfg)
	if err != nil {
		return nil, "", err
	}

	// Test the credentials reload func, i.e. get the current credentials, which
	// might just be a static credentials in the Blip config file or another file,
	// but it could be something dynamic like an Amazon IAM auth token.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	credentials, err := credentialFunc(ctx)
	if err != nil {
		return nil, "", err
	}

	// Credentials are username:password--part of the DSN created below
	cred := credentials.Username
	if credentials.Password != "" {
		cred += ":" + credentials.Password
	}

	// ----------------------------------------------------------------------
	// Load TLS

	params := []string{"parseTime=true", "interpolateParams=true"}

	// Go says "either ServerName or InsecureSkipVerify must be specified".
	// This is a pathological case: socket and TLS but no hostname to verify
	// and user didn't explicitly set skip-verify=true. So we set this latter
	// automatically because Go will certainly error if we don't.
	if net == "unix" && cfg.TLS.Set() && cfg.Hostname == "" && !blip.True(cfg.TLS.SkipVerify) {
		b := true
		cfg.TLS.SkipVerify = &b
		blip.Debug("%s: auto-enabled skip-verify on socket with TLS but no hostname", cfg.MonitorId)
	}

	// Load and register TLS, if any
	tlsConfig, err := cfg.TLS.LoadTLS(portSuffix.ReplaceAllString(cfg.Hostname, ""))
	if err != nil {
		return nil, "", err
	}

	if tlsConfig != nil {
		mysql.RegisterTLSConfig(cfg.MonitorId, tlsConfig)
		params = append(params, "tls="+cfg.MonitorId)
		blip.Debug("%s: TLS enabled", cfg.MonitorId)

		// TLS is configured, so make sure we reload it when the credentials are reloaded in case
		// it was changed
		origCredentialFunc := credentialFunc
		credentialFunc = func(ctx context.Context) (Credentials, error) {
			creds, err := origCredentialFunc(ctx)
			if err != nil {
				return creds, err
			}

			// Get a copy of the original configuration and apply the
			// TLS configuration to it as a default value. If blipConfig
			// already has TLS settings in place then creds.TLS is ignored
			blipConfig := originalCfg
			blipConfig.ApplyDefaults(blip.Config{TLS: creds.TLS})

			// Determine if we need to set SkipVerify
			if blipConfig.Socket != "" && blipConfig.TLS.Set() && blipConfig.Hostname == "" && !blip.True(blipConfig.TLS.SkipVerify) {
				b := true
				blipConfig.TLS.SkipVerify = &b
				blip.Debug("%s: auto-enabled skip-verify on socket with TLS but no hostname", blipConfig.MonitorId)
			}

			tlsConfig, err := blipConfig.TLS.LoadTLS(portSuffix.ReplaceAllString(blipConfig.Hostname, ""))
			if err != nil {
				// Don't interrupt the password reload if we have an error loading TLS.
				// If there was a change we will get an error when trying to re-connect
				// so log the error and continue
				log.Printf("Error reloading TLS settings: %v", err)

				return creds, nil
			}

			// Register then TLS config, if we have any. If the user previously specified TLS settings
			// then they should still exist now. If they don't we will leave the old settings in place,
			// which may generate an error when Blip tries to reconnect.
			if tlsConfig != nil {
				mysql.RegisterTLSConfig(blipConfig.MonitorId, tlsConfig)
				blip.Debug("%s: Re-registring TLS", blipConfig.MonitorId)
			}

			return creds, nil
		}
	}

	// Use built-in Amazon RDS CA if password is AWS IAM auth or Secrets Manager
	// and auto-TLS is still enabled (default) and user didn't provide an explicit
	// TLS config (above). This latter is really forward-looking: Amazon rotates
	// its certs, so eventually the Blip built-in will be out of date. But user
	// will never be blocked (waiting for a new Blip release) because they can
	// override the built-in Amazon cert.
	if (blip.True(cfg.AWS.IAMAuth) || cfg.AWS.PasswordSecret != "") &&
		!blip.True(cfg.AWS.DisableAutoTLS) &&
		tlsConfig == nil {

		blip.Debug("%s: auto AWS TLS: using IAM auth or Secrets Manager", cfg.MonitorId)
		aws.RegisterRDSCA() // safe to call multiple times
		params = append(params, "tls=rds")
	}

	if rdsAddr.MatchString(addr) && !blip.True(cfg.AWS.DisableAutoTLS) && tlsConfig == nil {
		blip.Debug("%s: auto AWS TLS: hostname has suffix .rds.amazonaws.com", cfg.MonitorId)
		aws.RegisterRDSCA() // safe to call multiple times
		params = append(params, "tls=rds")
	}

	// ----------------------------------------------------------------------
	// IAM auto requires cleartext passwords (the auth token is already encryopted)

	if blip.True(cfg.AWS.IAMAuth) {
		params = append(params, "allowCleartextPasswords=true")
	}

	// ----------------------------------------------------------------------
	// Create DSN and *sql.DB

	dsn := fmt.Sprintf("%s@%s(%s)/", cred, net, addr)
	if len(params) > 0 {
		dsn += "?" + strings.Join(params, "&")
	}

	// mysql-hotswap-dsn is a special driver; see reload_password.go.
	// Remember: this does NOT connect to MySQL; it only creates a valid
	// *sql.DB connection pool. Since the caller is Monitor.Run (indirectly
	// via the blip.DbFactory it was given), actually connecting to MySQL
	// happens (probably) by monitor/Engine.Prepare, or possibly by other
	// components (plan loader, LPA, heartbeat, etc.)
	db, err := sql.Open("mysql-hotswap-dsn", dsn)
	if err != nil {
		return nil, "", err
	}

	// ======================================================================
	// Valid db/DSN, do not return error past here
	// ======================================================================

	// Now that we know the DSN/DB are valid, register the credential reload func.
	// Don't do this earlier becuase there's no way to unregister it, which is
	// probably a bug/leak if/when Blip allows dyanmically unloading monitors.
	Repo.Add(addr, credentialFunc)

	// Limit Blip to 3 MySQL conn by default: 1 or 2 for metrics, and 1 for
	// LPA, heartbeat, etc. Since all metrics are supposed to collect in a
	// matter of milliseconds, 3 should be more than enough.
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(3)

	// Let user-provided plugin set/change DB
	if f.modifyDB != nil {
		f.modifyDB(db, dsn)
	}

	return db, RedactedDSN(dsn), nil
}

// Credentials creates a credentials reload function (callback) based on the
// configured credential method. This function is used by the mysql-hotswap-dsn
// driver (see reload_password.go). For a consistent abstraction, all
// credentials are fetched via a reload func, even a static credential specified
// in the Blip config file.
func (f factory) Credentials(cfg blip.ConfigMonitor) (CredentialFunc, error) {

	// Amazon IAM auth token (valid 15 min)
	if blip.True(cfg.AWS.IAMAuth) {
		blip.Debug("%s: AWS IAM auth token password", cfg.MonitorId)
		awscfg, err := f.awsConfig.Make(blip.AWS{Region: cfg.AWS.Region}, cfg.Hostname)
		if err != nil {
			return nil, err
		}
		token := aws.NewAuthToken(cfg.Username, cfg.Hostname, awscfg)
		return func(ctx context.Context) (Credentials, error) {
			passwd, err := token.Password(ctx)
			if err != nil {
				return Credentials{}, err
			}

			return Credentials{
				Password: passwd,
				Username: cfg.Username,
			}, nil
		}, nil
	}

	// Amazon Secrets Manager, could be rotated
	if cfg.AWS.PasswordSecret != "" {
		blip.Debug("%s: AWS Secrets Manager password", cfg.MonitorId)
		awscfg, err := f.awsConfig.Make(blip.AWS{Region: cfg.AWS.Region}, cfg.Hostname)
		if err != nil {
			return nil, err
		}
		secret := aws.NewSecret(cfg.AWS.PasswordSecret, awscfg)
		return func(ctx context.Context) (Credentials, error) {
			newSecret, err := secret.GetSecret(ctx)
			if err != nil {
				return Credentials{}, err
			}

			username, ok := newSecret["username"]
			if !ok {
				// The username key is optional. Default to config
				username = cfg.Username
			}
			usernameStr, ok := username.(string)
			if !ok {
				username = cfg.Username
			}
			password, ok := newSecret["password"]
			if !ok {
				return Credentials{}, fmt.Errorf("error retrieving 'password' value of secret")
			}
			passwordStr, ok := password.(string)
			if !ok {
				return Credentials{}, fmt.Errorf("invalid type for 'password' value of secret")
			}

			return Credentials{
				Password: passwordStr,
				Username: usernameStr,
			}, nil
		}, nil
	}

	// Password file, could be "rotated" (new password written to file)
	if cfg.PasswordFile != "" {
		blip.Debug("%s: password file", cfg.MonitorId)
		return func(context.Context) (Credentials, error) {
			bytes, err := os.ReadFile(cfg.PasswordFile)
			if err != nil {
				return Credentials{}, err
			}
			return Credentials{
				Password: string(bytes),
				Username: cfg.Username,
			}, err
		}, nil
	}

	// Credentials in my.cnf file, could be rotated (username and/or password, along with TLS config)
	if cfg.MyCnf != "" {
		blip.Debug("%s my.cnf credentials", cfg.MonitorId)
		return func(context.Context) (Credentials, error) {
			cfg, tlscfg, err := ParseMyCnf(cfg.MyCnf)
			if err != nil {
				return Credentials{}, err
			}
			return Credentials{
				Password: cfg.Password,
				Username: cfg.Username,
				TLS:      tlscfg,
			}, err
		}, nil
	}

	// Static password in Blip config file, not rotated
	if cfg.Password != "" {
		blip.Debug("%s: static password credentials", cfg.MonitorId)
		return func(context.Context) (Credentials, error) {
			return Credentials{Password: cfg.Password, Username: cfg.Username}, nil
		}, nil
	}

	blip.Debug("%s: no password", cfg.MonitorId)
	return func(context.Context) (Credentials, error) {
		return Credentials{Password: "", Username: cfg.Username}, nil
	}, nil
}

// --------------------------------------------------------------------------

const (
	default_mysql_socket  = "/tmp/mysql.sock"
	default_distro_socket = "/var/lib/mysql/mysql.sock"
)

func Sockets() []string {
	sockets := []string{}
	seen := map[string]bool{}
	for _, socket := range strings.Split(socketList(), "\n") {
		socket = strings.TrimSpace(socket)
		if socket == "" {
			continue
		}
		if seen[socket] {
			continue
		}
		seen[socket] = true
		if !isSocket(socket) {
			continue
		}
		sockets = append(sockets, socket)
	}

	if len(sockets) == 0 {
		blip.Debug("no sockets, using defaults")
		if isSocket(default_mysql_socket) {
			sockets = append(sockets, default_mysql_socket)
		}
		if isSocket(default_distro_socket) {
			sockets = append(sockets, default_distro_socket)
		}
	}

	blip.Debug("sockets: %v", sockets)
	return sockets
}

func socketList() string {
	cmd := exec.Command("sh", "-c", "netstat -f unix | grep mysql | grep -v mysqlx | awk '{print $NF}'")
	output, err := cmd.Output()
	if err != nil {
		blip.Debug(err.Error())
	}
	return string(output)
}

func isSocket(file string) bool {
	fi, err := os.Stat(file)
	if err != nil {
		return false
	}
	return fi.Mode()&fs.ModeSocket != 0
}

func RedactedDSN(dsn string) string {
	redactedPassword, err := mysql.ParseDSN(dsn)
	if err != nil { // ok to ignore
		blip.Debug("mysql.ParseDSN error: %s", err)
	}
	redactedPassword.Passwd = "..."
	return redactedPassword.FormatDSN()

}
