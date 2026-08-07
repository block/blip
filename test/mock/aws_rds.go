// Copyright 2024 Block, Inc.

package mock

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/rds"

	"github.com/cashapp/blip/v2"
	blipAWS "github.com/cashapp/blip/v2/aws"
)

type RDSClient struct {
	Out   rds.DescribeDBInstancesOutput
	Error error
}

func (r RDSClient) DescribeDBInstances(context.Context, *rds.DescribeDBInstancesInput, ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error) {
	return &r.Out, r.Error
}

type RDSClientFactory struct {
	MakeFunc func(blip.AWS) (blipAWS.RDSClient, error)
}

func (f RDSClientFactory) Make(ba blip.AWS) (blipAWS.RDSClient, error) {
	if f.MakeFunc != nil {
		return f.MakeFunc(ba)
	}
	return RDSClient{}, nil
}
