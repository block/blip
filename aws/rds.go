// Copyright 2024 Block, Inc.

package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/rds"

	"github.com/cashapp/blip/v2"
)

type RDSClient interface {
	// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/rds#DescribeDBInstancesAPIClient
	rds.DescribeDBInstancesAPIClient
}

type RDSClientFactory interface {
	Make(blip.AWS) (RDSClient, error)
}

type rdsClientFactory struct {
	awsMaker blip.AWSConfigFactory
}

func NewRDSClientFactory(awsMaker blip.AWSConfigFactory) rdsClientFactory {
	return rdsClientFactory{
		awsMaker: awsMaker,
	}
}

func (f rdsClientFactory) Make(ba blip.AWS) (RDSClient, error) {
	awsCfg, err := f.awsMaker.Make(ba, "")
	if err != nil {
		return nil, err
	}
	client := rds.NewFromConfig(awsCfg)
	return client, nil
}

// --------------------------------------------------------------------------

type RDSLoader struct {
	ClientFactory RDSClientFactory
}

// Load calls DescribeDBInstances to return a list of RDS instances.
// This is equivalent to the command "aws rds describe-db-instances".
func (rl RDSLoader) Load(ctx context.Context, cfg blip.Config) ([]blip.ConfigMonitor, error) {
	loaderCfg := cfg.MonitorLoader.AWS
	if len(loaderCfg.Regions) == 0 {
		return nil, fmt.Errorf("no regions specififed")
	}

	mons := []blip.ConfigMonitor{}

	for _, region := range loaderCfg.Regions {
		client, err := rl.ClientFactory.Make(blip.AWS{Region: region})
		if err != nil {
			return nil, err
		}

		var marker string // pagination
	PAGES:
		for {
			// Query RDS API to list all db instances. Marker (for pagination) is set
			// (not an empty string) by previous calls--see below.
			out, err := client.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{Marker: &marker})
			if err != nil {
				return nil, err
			}

			// Make a blip.ConfigMonitor for every RDS instances
			blip.Debug("%d db instances", len(out.DBInstances))
			for _, instance := range out.DBInstances {
				// During provision or decommission, endpoint can be nil
				if instance.Endpoint == nil || instance.Endpoint.Address == nil {
					blip.Debug("%s endpoint nil (status=%v)", *instance.DBInstanceIdentifier, *instance.DBInstanceStatus)
					continue
				}
				monCfg := blip.ConfigMonitor{
					MonitorId: *instance.DBInstanceIdentifier,
					Hostname:  fmt.Sprintf("%s:%d", *instance.Endpoint.Address, instance.Endpoint.Port),
					AWS: blip.ConfigAWS{
						Region: region,
					},
				}
				mons = append(mons, monCfg)
				blip.Debug("loaded %si dbid=%v cluster=%v version=%v az=%v stauts=%v ",
					monCfg.Hostname, *instance.DBInstanceIdentifier, *instance.DBClusterIdentifier, *instance.EngineVersion, *instance.AvailabilityZone, *instance.DBInstanceStatus)
			}

			// Max 100 instances per page; read next page if marker is set
			if out.Marker != nil {
				blip.Debug("next page")
				marker = *out.Marker
				continue PAGES // next page
			}

			break PAGES // last page
		}
	}

	return mons, nil
}
