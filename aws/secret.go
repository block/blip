// Copyright 2024 Block, Inc.

package aws

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/cashapp/blip"
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

func (s Secret) Username(v map[string]interface{}) (string, error) {
	username, ok := v["username"]
	if !ok {
		return "", fmt.Errorf("failed to retrieve value for key 'username'")
	}
	return username.(string), nil
}

func (s Secret) Password(v map[string]interface{}) (string, error) {
	username, ok := v["password"]
	if !ok {
		return "", fmt.Errorf("failed to retrieve value for key 'password'")
	}
	return username.(string), nil
}

func (s Secret) GetSecret(ctx context.Context) (map[string]interface{}, error) {
	input := &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(s.name),
		VersionStage: aws.String("AWSCURRENT"),
	}

	sv, err := s.client.GetSecretValue(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("Secrets Manager API error: %s", err)
	}
	blip.Debug("DEBUG: aws secret: %+v", *sv)

	if sv.SecretString == nil || *sv.SecretString == "" {
		return nil, fmt.Errorf("secret string is nil or empty")
	}

	var v map[string]interface{}
	if err := json.Unmarshal([]byte(*sv.SecretString), &v); err != nil {
		return nil, fmt.Errorf("cannot decode secret string as map[string]string: %s", err)
	}
	if v == nil {
		return nil, fmt.Errorf("secret value is 'null' literal")
	}
	return v, nil
}
