// Copyright 2024 Block, Inc.

package aws

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"

	"github.com/cashapp/blip/v2"
)

type ConfigFactory struct {
	region string
}

func (f *ConfigFactory) Make(ba blip.AWS, endpoint string) (aws.Config, error) {
	if ba.Region == "auto" {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		var err error
		ba.Region, err = Region(ctx)
		if err != nil {
			blip.Debug("cannot auto-detect AWS region: %s", err)
			return aws.Config{}, fmt.Errorf("cannot auto-detect AWS region (EC2 IMDS query failed)")
		}
		if f.region == "" {
			blip.Debug("set factory region to %s", ba.Region)
			f.region = ba.Region
		}
	} else if f.region != "" {
		blip.Debug("using factory AWS region: %s", f.region)
		ba.Region = f.region
	} else if endpoint != "" {
		//          0         1         2   3         4   5
		// <instance>.<cluster>.us-east-1.rds.amazonaws.com
		f := strings.Split(endpoint, ".")
		if len(f) != 6 {
			blip.Debug("AWS region from splitting %s returned %d fields, expected 6: %#v", endpoint, len(f), f)
		} else {
			ba.Region = f[2]
			blip.Debug("AWS region from %s: %s", endpoint, ba.Region)
		}
	} else {
		blip.Debug("AWS region auto off and no factory region; AWS creds will probably fail")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return config.LoadDefaultConfig(ctx, config.WithRegion(ba.Region))
}

// Region auto-detects the region. Currently, the function relies on IMDS v2:
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
// If the region cannot be detect, it returns an empty string.
func Region(ctx context.Context) (string, error) {
	blip.Debug("auto-detect AWS region from IMDS")
	client := imds.New(imds.Options{})
	ec2, err := client.GetInstanceIdentityDocument(ctx, &imds.GetInstanceIdentityDocumentInput{})
	if err != nil {
		return "", err
	}
	return ec2.Region, nil
}
