// Copyright 2026 Block, Inc.

package credentials_test

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/credentials"
)

type staticAWSConfigFactory struct {
	config awsv2.Config
}

func (f staticAWSConfigFactory) Make(blip.AWS, string) (awsv2.Config, error) {
	return f.config, nil
}

func TestDynamicPasswordFileReloads(t *testing.T) {
	passwordFile := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(passwordFile, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}

	credentialFunc, selected, err := credentials.NewFactory(nil, nil).Dynamic(blip.ConfigMonitor{
		Username:     "metrics",
		PasswordFile: passwordFile,
	}, "3306")
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
			credentialFunc, selected, err := credentials.NewFactory(nil, nil).Dynamic(tt.config, "3306")
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

func TestDynamicIAMUsesCallerDefaultPort(t *testing.T) {
	iamAuth := true
	factory := credentials.NewFactory(staticAWSConfigFactory{config: awsv2.Config{
		Region: "us-west-2",
		Credentials: awsv2.CredentialsProviderFunc(func(context.Context) (awsv2.Credentials, error) {
			return awsv2.Credentials{
				AccessKeyID:     "test-access-key-do-not-use",
				SecretAccessKey: "test-secret-key-do-not-use",
				SessionToken:    "test-session-token-do-not-use",
			}, nil
		}),
	}}, nil)

	tests := []struct {
		name        string
		defaultPort string
		hostname    string
		wantHost    string
	}{
		{
			name:        "MySQL default",
			defaultPort: "3306",
			hostname:    "mysql.example",
			wantHost:    "mysql.example:3306",
		},
		{
			name:        "external engine default",
			defaultPort: "5432",
			hostname:    "postgres.example",
			wantHost:    "postgres.example:5432",
		},
		{
			name:        "explicit port",
			defaultPort: "5432",
			hostname:    "postgres.example:6432",
			wantHost:    "postgres.example:6432",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			credentialFunc, selected, err := factory.Dynamic(blip.ConfigMonitor{
				Hostname: tt.hostname,
				Username: "metrics",
				AWS: blip.ConfigAWS{
					IAMAuth: &iamAuth,
					Region:  "us-west-2",
				},
			}, tt.defaultPort)
			if err != nil {
				t.Fatal(err)
			}
			if !selected {
				t.Fatal("IAM credentials were not selected")
			}

			got, err := credentialFunc(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			tokenURL, err := url.Parse("https://" + got.Password)
			if err != nil {
				t.Fatal(err)
			}
			if tokenURL.Host != tt.wantHost {
				t.Fatalf("IAM token host = %q, expected %q", tokenURL.Host, tt.wantHost)
			}
		})
	}
}

func TestDynamicIAMRequiresCallerDefaultPort(t *testing.T) {
	iamAuth := true
	factory := credentials.NewFactory(staticAWSConfigFactory{config: awsv2.Config{Region: "us-west-2"}}, nil)
	credentialFunc, selected, err := factory.Dynamic(blip.ConfigMonitor{
		Hostname: "database.example",
		Username: "metrics",
		AWS: blip.ConfigAWS{
			IAMAuth: &iamAuth,
			Region:  "us-west-2",
		},
	}, "")
	if !selected || credentialFunc != nil || err == nil || !strings.Contains(err.Error(), "default port") {
		t.Fatalf("Dynamic returned func=%v selected=%t err=%v", credentialFunc != nil, selected, err)
	}
}

func TestDynamicReportsNoSharedSource(t *testing.T) {
	credentialFunc, selected, err := credentials.NewFactory(nil, nil).Dynamic(blip.ConfigMonitor{
		Username: "metrics",
		Password: "static",
	}, "3306")
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
