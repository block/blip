// Copyright 2024 Block, Inc.

package dbconn

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/cashapp/blip"
)

type testAWSConfigFactory struct {
	cfg aws.Config
	err error
}

func (f testAWSConfigFactory) Make(blip.AWS, string) (aws.Config, error) {
	if f.err != nil {
		return aws.Config{}, f.err
	}
	return f.cfg, nil
}

type errHTTPClient struct {
	err error
}

func (c errHTTPClient) Do(*http.Request) (*http.Response, error) {
	return nil, c.err
}

func testPasswordSecretConfig(t *testing.T, secretString string) (aws.Config, func()) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		if err := json.NewEncoder(w).Encode(map[string]string{
			"Name":         "test-secret",
			"SecretString": secretString,
			"VersionId":    "test-version",
		}); err != nil {
			t.Errorf("cannot encode Secrets Manager response: %s", err)
		}
	}))

	return aws.Config{
		Credentials: credentials.NewStaticCredentialsProvider("access-key", "secret-key", ""),
		EndpointResolver: aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			return aws.Endpoint{URL: server.URL, SigningRegion: "us-east-1"}, nil
		}),
		HTTPClient: server.Client(),
		Region:     "us-east-1",
		Retryer: func() aws.Retryer {
			return aws.NopRetryer{}
		},
	}, server.Close
}

func TestPasswordSecretCredentialFuncDefaultParser(t *testing.T) {
	awscfg, cleanup := testPasswordSecretConfig(t, `{"password":"secret-pass"}`)
	defer cleanup()

	f := factory{awsConfig: testAWSConfigFactory{cfg: awscfg}}
	credentialFunc, err := f.Credentials(blip.ConfigMonitor{
		Hostname: "db.example.com",
		Username: "config-user",
		AWS: blip.ConfigAWS{
			PasswordSecret: "test-secret",
			Region:         "us-east-1",
		},
	})
	if err != nil {
		t.Fatalf("got error %s, expected nil", err)
	}

	creds, err := credentialFunc(context.Background())
	if err != nil {
		t.Fatalf("got error %s, expected nil", err)
	}
	if creds.Username != "config-user" {
		t.Errorf("Username=%q, expected config-user", creds.Username)
	}
	if creds.Password != "secret-pass" {
		t.Errorf("Password=%q, expected secret-pass", creds.Password)
	}
}

func TestPasswordSecretCredentialFuncCustomParser(t *testing.T) {
	awscfg, cleanup := testPasswordSecretConfig(t, "secret-user:secret-pass")
	defer cleanup()

	called := false
	f := factory{
		awsConfig: testAWSConfigFactory{cfg: awscfg},
		passwordSecretParser: func(_ context.Context, cfg blip.ConfigMonitor, payload []byte, credentials *blip.DbCredentials) error {
			called = true
			if cfg.Username != "config-user" {
				t.Errorf("cfg.Username=%q, expected config-user", cfg.Username)
			}
			if credentials.Username != "config-user" {
				t.Errorf("pre-populated Username=%q, expected config-user", credentials.Username)
			}
			if string(payload) != "secret-user:secret-pass" {
				t.Errorf("payload=%q, expected secret-user:secret-pass", string(payload))
			}
			credentials.Username = "secret-user"
			credentials.Password = "secret-pass"
			return nil
		},
	}
	credentialFunc, err := f.Credentials(blip.ConfigMonitor{
		Hostname: "db.example.com",
		Username: "config-user",
		AWS: blip.ConfigAWS{
			PasswordSecret: "test-secret",
			Region:         "us-east-1",
		},
	})
	if err != nil {
		t.Fatalf("got error %s, expected nil", err)
	}

	creds, err := credentialFunc(context.Background())
	if err != nil {
		t.Fatalf("got error %s, expected nil", err)
	}
	if !called {
		t.Fatal("custom parser was not called")
	}
	if creds.Username != "secret-user" {
		t.Errorf("Username=%q, expected secret-user", creds.Username)
	}
	if creds.Password != "secret-pass" {
		t.Errorf("Password=%q, expected secret-pass", creds.Password)
	}
}

func TestPasswordSecretCredentialFuncParserError(t *testing.T) {
	awscfg, cleanup := testPasswordSecretConfig(t, "secret-pass")
	defer cleanup()

	parseErr := errors.New("parse secret")
	f := factory{
		awsConfig: testAWSConfigFactory{cfg: awscfg},
		passwordSecretParser: func(context.Context, blip.ConfigMonitor, []byte, *blip.DbCredentials) error {
			return parseErr
		},
	}
	credentialFunc, err := f.Credentials(blip.ConfigMonitor{
		Hostname: "db.example.com",
		Username: "config-user",
		AWS: blip.ConfigAWS{
			PasswordSecret: "test-secret",
			Region:         "us-east-1",
		},
	})
	if err != nil {
		t.Fatalf("got error %s, expected nil", err)
	}

	_, err = credentialFunc(context.Background())
	if !errors.Is(err, parseErr) {
		t.Fatalf("got error %v, expected %v", err, parseErr)
	}
}

func TestPasswordSecretCredentialFuncGetSecretError(t *testing.T) {
	getErr := errors.New("get secret")
	f := factory{
		awsConfig: testAWSConfigFactory{cfg: aws.Config{
			Credentials: credentials.NewStaticCredentialsProvider("access-key", "secret-key", ""),
			EndpointResolver: aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://127.0.0.1", SigningRegion: "us-east-1"}, nil
			}),
			HTTPClient: errHTTPClient{err: getErr},
			Region:     "us-east-1",
			Retryer: func() aws.Retryer {
				return aws.NopRetryer{}
			},
		}},
	}
	credentialFunc, err := f.Credentials(blip.ConfigMonitor{
		Hostname: "db.example.com",
		Username: "config-user",
		AWS: blip.ConfigAWS{
			PasswordSecret: "test-secret",
			Region:         "us-east-1",
		},
	})
	if err != nil {
		t.Fatalf("got error %s, expected nil", err)
	}

	_, err = credentialFunc(context.Background())
	if err == nil || !strings.Contains(err.Error(), getErr.Error()) {
		t.Fatalf("got error %v, expected %v", err, getErr)
	}
}

func TestPasswordSecretCredentialFuncAWSConfigError(t *testing.T) {
	configErr := errors.New("aws config")
	f := factory{awsConfig: testAWSConfigFactory{err: configErr}}

	_, err := f.Credentials(blip.ConfigMonitor{
		Hostname: "db.example.com",
		Username: "config-user",
		AWS: blip.ConfigAWS{
			PasswordSecret: "test-secret",
			Region:         "us-east-1",
		},
	})
	if !errors.Is(err, configErr) {
		t.Fatalf("got error %v, expected %v", err, configErr)
	}
}
