// Copyright 2024 Block, Inc.

package aws

import (
	"context"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
)

var portRe = regexp.MustCompile(`:\d+$`)

type AuthToken struct {
	username string
	hostname string
	cfg      aws.Config
}

func NewAuthToken(username, hostname string, cfg aws.Config) AuthToken {
	return NewAuthTokenWithDefaultPort(username, hostname, "3306", cfg)
}

// NewAuthTokenWithDefaultPort constructs an RDS authentication token signer,
// adding defaultPort when hostname does not already include a port.
func NewAuthTokenWithDefaultPort(username, hostname, defaultPort string, cfg aws.Config) AuthToken {
	// RDS auth tokens require the database port in the signed endpoint.
	if !portRe.MatchString(hostname) {
		hostname += ":" + defaultPort
	}

	return AuthToken{
		username: username,
		hostname: hostname,
		cfg:      cfg,
	}
}

func (a AuthToken) Password(ctx context.Context) (string, error) {
	return auth.BuildAuthToken(
		ctx,
		a.hostname,
		a.cfg.Region,
		a.username,
		a.cfg.Credentials,
	)
}
