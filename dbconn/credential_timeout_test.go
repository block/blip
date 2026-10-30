// Copyright 2026 Block, Inc.

package dbconn

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/cashapp/blip/v2"
)

// slowCredentialsConfig returns an AWS config for a Secrets Manager stub
// with AWS credentials that take delay to fetch, like the first IRSA
// AssumeRoleWithWebIdentity call.
func slowCredentialsConfig(t *testing.T, delay time.Duration) aws.Config {
	t.Helper()
	awscfg, cleanup := testPasswordSecretConfig(t, `{"password":"secret-pass"}`)
	t.Cleanup(cleanup)
	awscfg.Credentials = aws.NewCredentialsCache(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		select {
		case <-time.After(delay):
			return aws.Credentials{AccessKeyID: "access-key", SecretAccessKey: "secret-key", Source: "test"}, nil
		case <-ctx.Done():
			return aws.Credentials{}, ctx.Err()
		}
	}))
	return awscfg
}

func passwordSecretMonitor() blip.ConfigMonitor {
	return blip.ConfigMonitor{
		MonitorId: "m1",
		Hostname:  "127.0.0.1:33997",
		Username:  "config-user",
		AWS: blip.ConfigAWS{
			PasswordSecret: "test-secret",
			Region:         "us-east-1",
		},
	}
}

func TestMakeCredentialTimeoutDefault(t *testing.T) {
	// The first credentials from AWS can take seconds (fetching AWS credentials
	// via IRSA, AssumeRole, etc.), so Make must wait longer than the old 500ms.
	awscfg := slowCredentialsConfig(t, 700*time.Millisecond)
	f := NewConnFactory(testAWSConfigFactory{cfg: awscfg}, nil)
	if f.credentialTimeout != DefaultCredentialTimeout {
		t.Errorf("credentialTimeout=%s, expected %s", f.credentialTimeout, DefaultCredentialTimeout)
	}

	db, _, err := f.Make(passwordSecretMonitor())
	if err != nil {
		t.Fatalf("got error %s, expected nil", err)
	}
	db.Close()
}

func TestMakeCredentialTimeoutOption(t *testing.T) {
	awscfg := slowCredentialsConfig(t, 2*time.Second)
	f := NewConnFactory(testAWSConfigFactory{cfg: awscfg}, nil, WithCredentialTimeout(50*time.Millisecond))

	t0 := time.Now()
	db, _, err := f.Make(passwordSecretMonitor())
	elapsed := time.Since(t0)
	if err == nil {
		db.Close()
		t.Fatal("got nil error, expected credential timeout")
	}
	if elapsed > 5*time.Second {
		t.Errorf("Make took %s, expected about 50ms", elapsed)
	}

	// The SDK error must survive wrapping so callers can tell a timeout or
	// cancellation from, e.g., a bad secret
	var canceled *aws.RequestCanceledError
	if !errors.As(err, &canceled) {
		t.Errorf("errors.As(%v, *aws.RequestCanceledError) = false, expected true", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("errors.Is(%v, context.DeadlineExceeded) = false, expected true", err)
	}
}

func TestMakeCredentialTimeoutDisabled(t *testing.T) {
	awscfg := slowCredentialsConfig(t, 700*time.Millisecond)
	f := NewConnFactory(testAWSConfigFactory{cfg: awscfg}, nil, WithCredentialTimeout(0))

	db, _, err := f.Make(passwordSecretMonitor())
	if err != nil {
		t.Fatalf("got error %s, expected nil", err)
	}
	db.Close()
}
