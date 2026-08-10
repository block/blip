// Copyright 2024 Block, Inc.

package aws_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"

	//"github.com/stretchr/testify/assert"

	"github.com/cashapp/blip/v2"
	blipAWS "github.com/cashapp/blip/v2/aws"
	"github.com/cashapp/blip/v2/test/mock"
)

func TestRDSClient(t *testing.T) {
	client := mock.RDSClient{
		Out: rds.DescribeDBInstancesOutput{
			DBInstances: []types.DBInstance{
				{
					// Need ALL these fields else a debug statement will panic
					DBInstanceIdentifier: aws.String("rds1"),
					DBClusterIdentifier:  aws.String("rds-001"),
					Endpoint: &types.Endpoint{
						Address: aws.String("rds1"),
						Port:    3306,
					},

					EngineVersion:    aws.String("v8.0.0"),
					AvailabilityZone: aws.String("us-west-2a"),
					DBInstanceStatus: aws.String("fantastic"),
				},
				{
					DBInstanceIdentifier: aws.String("rds2"),
					DBClusterIdentifier:  aws.String("rds-001"),
					Endpoint: &types.Endpoint{
						Address: aws.String("rds2"),
						Port:    3307,
					},

					EngineVersion:    aws.String("v8.0.0"),
					AvailabilityZone: aws.String("us-west-2a"),
					DBInstanceStatus: aws.String("fantastic"),
				},
			},
		},
	}
	f := mock.RDSClientFactory{
		MakeFunc: func(ba blip.AWS) (blipAWS.RDSClient, error) {
			return client, nil
		},
	}

	rdsLoader := blipAWS.RDSLoader{ClientFactory: f}

	cfg := blip.Config{
		MonitorLoader: blip.ConfigMonitorLoader{
			AWS: blip.ConfigMonitorLoaderAWS{
				Regions: []string{"us-west-2"},
			},
		},
	}
	got, err := rdsLoader.Load(context.Background(), cfg)
	if err != nil {
		t.Error(err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d ConfigMonitor, expected 2", len(got))
	}
	if got[0].Hostname != "rds1:3306" {
		t.Errorf("ConfigMonitor[0].Hostname = %s, expected rds1:3306", got[0].Hostname)
	}
	if got[1].Hostname != "rds2:3307" {
		t.Errorf("ConfigMonitor[1].Hostname = %s, expected rds2:3307", got[1].Hostname)
	}
}
