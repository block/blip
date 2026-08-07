// Copyright 2026 Block, Inc.

// Package credentials constructs engine-neutral database credential callbacks.
// Database connection packages remain responsible for engine-specific sources,
// caching, authentication-error detection, and connection retry behavior.
package credentials

import (
	"context"
	"fmt"
	"os"

	"github.com/cashapp/blip"
	blipaws "github.com/cashapp/blip/aws"
)

// Func returns the database credentials currently available from a configured
// source. Callers decide when to cache or refresh the result.
type Func func(context.Context) (blip.DbCredentials, error)

// Factory constructs callbacks for credential sources shared by database
// engines. A nil password-secret parser selects Blip's default RDS secret
// parser.
type Factory struct {
	awsConfig            blip.AWSConfigFactory
	passwordSecretParser blip.PasswordSecretParser
}

func NewFactory(awsConfig blip.AWSConfigFactory, passwordSecretParser blip.PasswordSecretParser) Factory {
	return Factory{
		awsConfig:            awsConfig,
		passwordSecretParser: passwordSecretParser,
	}
}

// Dynamic returns the first configured shared reloadable source in Blip's
// established precedence order: IAM, Secrets Manager, then password file. The
// boolean reports whether a source was selected. Engine-specific factories can
// insert their own sources before falling back to Static. defaultPort is used
// only when signing an IAM token for a hostname without an explicit port.
func (f Factory) Dynamic(cfg blip.ConfigMonitor, defaultPort string) (Func, bool, error) {
	if blip.True(cfg.AWS.IAMAuth) {
		blip.Debug("%s: AWS IAM auth token password", cfg.MonitorId)
		if f.awsConfig == nil {
			return nil, true, fmt.Errorf("AWS IAM authentication requires an AWS config factory")
		}
		awscfg, err := f.awsConfig.Make(blip.AWS{Region: cfg.AWS.Region}, cfg.Hostname)
		if err != nil {
			return nil, true, err
		}
		if defaultPort == "" {
			return nil, true, fmt.Errorf("AWS IAM authentication requires a database default port")
		}
		token := blipaws.NewAuthTokenWithDefaultPort(cfg.Username, cfg.Hostname, defaultPort, awscfg)
		return func(ctx context.Context) (blip.DbCredentials, error) {
			password, err := token.Password(ctx)
			if err != nil {
				return blip.DbCredentials{}, err
			}
			return blip.DbCredentials{Username: cfg.Username, Password: password}, nil
		}, true, nil
	}

	if cfg.AWS.PasswordSecret != "" {
		blip.Debug("%s: AWS Secrets Manager password", cfg.MonitorId)
		if f.awsConfig == nil {
			return nil, true, fmt.Errorf("AWS Secrets Manager credentials require an AWS config factory")
		}
		awscfg, err := f.awsConfig.Make(blip.AWS{Region: cfg.AWS.Region}, cfg.Hostname)
		if err != nil {
			return nil, true, err
		}
		secret := blipaws.NewSecret(cfg.AWS.PasswordSecret, awscfg)
		parser := f.passwordSecretParser
		if parser == nil {
			parser = blip.DefaultPasswordSecretParser
		}
		return func(ctx context.Context) (blip.DbCredentials, error) {
			payload, err := secret.GetSecretPayload(ctx)
			if err != nil {
				return blip.DbCredentials{}, err
			}
			credentials := blip.DbCredentials{Username: cfg.Username}
			if err := parser(ctx, cfg, payload, &credentials); err != nil {
				return blip.DbCredentials{}, err
			}
			return credentials, nil
		}, true, nil
	}

	if cfg.PasswordFile != "" {
		blip.Debug("%s: password file", cfg.MonitorId)
		return func(context.Context) (blip.DbCredentials, error) {
			contents, err := os.ReadFile(cfg.PasswordFile)
			if err != nil {
				return blip.DbCredentials{}, err
			}
			return blip.DbCredentials{Username: cfg.Username, Password: string(contents)}, nil
		}, true, nil
	}

	return nil, false, nil
}

// Static returns a callback for the configured static username and password.
// It also represents passwordless authentication when Password is empty.
func Static(cfg blip.ConfigMonitor) Func {
	if cfg.Password == "" {
		blip.Debug("%s: no password", cfg.MonitorId)
	} else {
		blip.Debug("%s: static password credentials", cfg.MonitorId)
	}
	return func(context.Context) (blip.DbCredentials, error) {
		return blip.DbCredentials{Username: cfg.Username, Password: cfg.Password}, nil
	}
}
