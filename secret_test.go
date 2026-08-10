// Copyright 2024 Block, Inc.

package blip_test

import (
	"context"
	"testing"

	"github.com/cashapp/blip/v2"
)

func TestDefaultPasswordSecretParser(t *testing.T) {
	tests := []struct {
		name           string
		payload        []byte
		expectUsername string
		expectPassword string
		expectErr      bool
	}{
		{
			name:           "username and password",
			payload:        []byte(`{"username":"secret-user","password":"secret-pass"}`),
			expectUsername: "secret-user",
			expectPassword: "secret-pass",
		},
		{
			name:           "password only",
			payload:        []byte(`{"password":"secret-pass"}`),
			expectUsername: "config-user",
			expectPassword: "secret-pass",
		},
		{
			name:           "non-string username falls back to config",
			payload:        []byte(`{"username":123,"password":"secret-pass"}`),
			expectUsername: "config-user",
			expectPassword: "secret-pass",
		},
		{
			name:      "missing password",
			payload:   []byte(`{"username":"secret-user"}`),
			expectErr: true,
		},
		{
			name:      "non-string password",
			payload:   []byte(`{"username":"secret-user","password":123}`),
			expectErr: true,
		},
		{
			name:      "malformed JSON",
			payload:   []byte(`{`),
			expectErr: true,
		},
		{
			name:      "null literal",
			payload:   []byte(`null`),
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := blip.ConfigMonitor{Username: "config-user"}
			credentials := blip.DbCredentials{}
			err := blip.DefaultPasswordSecretParser(context.Background(), cfg, tt.payload, &credentials)
			if tt.expectErr {
				if err == nil {
					t.Fatal("got nil error, expected non-nil error")
				}
				return
			}
			if err != nil {
				t.Fatalf("got error %s, expected nil", err)
			}
			if credentials.Username != tt.expectUsername {
				t.Errorf("Username=%q, expected %q", credentials.Username, tt.expectUsername)
			}
			if credentials.Password != tt.expectPassword {
				t.Errorf("Password=%q, expected %q", credentials.Password, tt.expectPassword)
			}
		})
	}
}

func TestDefaultPasswordSecretParserNilCredentials(t *testing.T) {
	err := blip.DefaultPasswordSecretParser(context.Background(), blip.ConfigMonitor{}, []byte(`{}`), nil)
	if err == nil {
		t.Fatal("got nil error, expected non-nil error")
	}
}
