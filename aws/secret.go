// Copyright 2024 Block, Inc.

package aws

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/cashapp/blip/v2"
)

type Secret struct {
	name   string
	client *secretsmanager.Client
}

func NewSecret(name string, cfg aws.Config) Secret {
	return Secret{
		name:   name,
		client: secretsmanager.NewFromConfig(cfg),
	}
}

func (s Secret) GetSecret(ctx context.Context) (map[string]interface{}, error) {
	payload, err := s.GetSecretPayload(ctx)
	if err != nil {
		return nil, err
	}

	var v map[string]interface{}
	if err := json.Unmarshal(payload, &v); err != nil {
		return nil, fmt.Errorf("cannot decode secret string as map[string]interface{}: %s", err)
	}
	if v == nil {
		return nil, fmt.Errorf("secret value is 'null' literal")
	}
	return v, nil
}

func (s Secret) GetSecretPayload(ctx context.Context) ([]byte, error) {
	input := &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(s.name),
		VersionStage: aws.String("AWSCURRENT"),
	}

	sv, err := s.client.GetSecretValue(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("Secrets Manager API error: %s", err)
	}
	name := ""
	if sv.Name != nil {
		name = *sv.Name
	}
	versionID := ""
	if sv.VersionId != nil {
		versionID = *sv.VersionId
	}
	blip.Debug("DEBUG: aws secret: name=%s version=%s", name, versionID)

	if sv.SecretString != nil && *sv.SecretString != "" {
		return []byte(*sv.SecretString), nil
	}
	if len(sv.SecretBinary) > 0 {
		return append([]byte(nil), sv.SecretBinary...), nil
	}

	return nil, fmt.Errorf("secret string and secret binary are empty")
}
