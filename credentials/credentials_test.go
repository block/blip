// Copyright 2026 Block, Inc.

package credentials_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/credentials"
)

func TestDynamicPasswordFileReloads(t *testing.T) {
	passwordFile := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(passwordFile, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}

	credentialFunc, selected, err := credentials.NewFactory(nil, nil).Dynamic(blip.ConfigMonitor{
		Username:     "metrics",
		PasswordFile: passwordFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !selected {
		t.Fatal("password file was not selected")
	}
	first, err := credentialFunc(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Username != "metrics" || first.Password != "first" {
		t.Fatalf("first credentials = %+v", first)
	}

	if err := os.WriteFile(passwordFile, []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := credentialFunc(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second.Username != "metrics" || second.Password != "second" {
		t.Fatalf("reloaded credentials = %+v", second)
	}
}

func TestDynamicPreservesSourcePrecedence(t *testing.T) {
	iamAuth := true
	tests := []struct {
		name      string
		config    blip.ConfigMonitor
		wantError string
	}{
		{
			name: "IAM before secret and file",
			config: blip.ConfigMonitor{
				PasswordFile: "/unused",
				AWS: blip.ConfigAWS{
					IAMAuth:        &iamAuth,
					PasswordSecret: "unused",
				},
			},
			wantError: "IAM authentication",
		},
		{
			name: "secret before file",
			config: blip.ConfigMonitor{
				PasswordFile: "/unused",
				AWS:          blip.ConfigAWS{PasswordSecret: "unused"},
			},
			wantError: "Secrets Manager",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			credentialFunc, selected, err := credentials.NewFactory(nil, nil).Dynamic(tt.config)
			if !selected {
				t.Fatal("configured source was not selected")
			}
			if credentialFunc != nil {
				t.Fatal("credential callback returned despite missing AWS factory")
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("got error %v, expected it to contain %q", err, tt.wantError)
			}
		})
	}
}

func TestDynamicReportsNoSharedSource(t *testing.T) {
	credentialFunc, selected, err := credentials.NewFactory(nil, nil).Dynamic(blip.ConfigMonitor{
		Username: "metrics",
		Password: "static",
	})
	if err != nil || selected || credentialFunc != nil {
		t.Fatalf("Dynamic returned func=%v selected=%t err=%v", credentialFunc != nil, selected, err)
	}
}

func TestStatic(t *testing.T) {
	credentialFunc := credentials.Static(blip.ConfigMonitor{Username: "metrics", Password: "static"})
	got, err := credentialFunc(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Username != "metrics" || got.Password != "static" {
		t.Fatalf("credentials = %+v", got)
	}
}
