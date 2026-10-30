// Copyright 2026 Block, Inc.

package aws_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"

	blipAWS "github.com/cashapp/blip/v2/aws"
)

func testSecretsManager(t *testing.T, handler http.HandlerFunc) aws.Config {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
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
	}
}

func TestGetSecretPayloadWrapsAPIError(t *testing.T) {
	cfg := testSecretsManager(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"__type":"ResourceNotFoundException","message":"no such secret"}`))
	})

	_, err := blipAWS.NewSecret("test-secret", cfg).GetSecretPayload(context.Background())
	if err == nil {
		t.Fatal("got nil error, expected API error")
	}
	if !strings.HasPrefix(err.Error(), "Secrets Manager API error: ") {
		t.Errorf("got error %q, expected prefix 'Secrets Manager API error: '", err)
	}
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		t.Errorf("errors.As(%v, *types.ResourceNotFoundException) = false, expected true", err)
	}
}

func TestGetSecretPayloadWrapsCanceled(t *testing.T) {
	// The first call might have to fetch AWS credentials (e.g. IRSA). If the
	// caller's context expires first, the SDK credentials cache returns
	// *aws.RequestCanceledError, which callers must be able to detect.
	cfg := testSecretsManager(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("Secrets Manager called without credentials")
	})
	cfg.Credentials = aws.NewCredentialsCache(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
		}
		return aws.Credentials{}, errors.New("slow credentials")
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := blipAWS.NewSecret("test-secret", cfg).GetSecretPayload(ctx)
	if err == nil {
		t.Fatal("got nil error, expected request canceled error")
	}
	var canceled *aws.RequestCanceledError
	if !errors.As(err, &canceled) {
		t.Errorf("errors.As(%v, *aws.RequestCanceledError) = false, expected true", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("errors.Is(%v, context.DeadlineExceeded) = false, expected true", err)
	}
}

func TestGetSecretWrapsDecodeError(t *testing.T) {
	cfg := testSecretsManager(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		json.NewEncoder(w).Encode(map[string]string{
			"Name":         "test-secret",
			"SecretString": "not-json",
			"VersionId":    "test-version",
		})
	})

	_, err := blipAWS.NewSecret("test-secret", cfg).GetSecret(context.Background())
	var syntaxErr *json.SyntaxError
	if !errors.As(err, &syntaxErr) {
		t.Errorf("errors.As(%v, *json.SyntaxError) = false, expected true", err)
	}
}
