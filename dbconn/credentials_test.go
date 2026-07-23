// Copyright 2026 Block, Inc.

package dbconn_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cashapp/blip"
	"github.com/cashapp/blip/dbconn"
)

func TestCredentialsPreserveMySQLMyCnfPrecedence(t *testing.T) {
	factory := dbconn.NewConnFactory(nil, nil)
	credentialFunc, err := factory.Credentials(blip.ConfigMonitor{
		Username: "static-user",
		Password: "static-password",
		MyCnf:    "../test/mycnf/full-dsn",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := credentialFunc(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Username != "U" || got.Password != "P" {
		t.Fatalf("credentials = %+v, expected my.cnf credentials", got)
	}
}

func TestCredentialsPreservePasswordFileBeforeMyCnf(t *testing.T) {
	passwordFile := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(passwordFile, []byte("file-password"), 0o600); err != nil {
		t.Fatal(err)
	}
	factory := dbconn.NewConnFactory(nil, nil)
	credentialFunc, err := factory.Credentials(blip.ConfigMonitor{
		Username:     "file-user",
		PasswordFile: passwordFile,
		MyCnf:        "../test/mycnf/full-dsn",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := credentialFunc(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Username != "file-user" || got.Password != "file-password" {
		t.Fatalf("credentials = %+v, expected password-file credentials", got)
	}
}
