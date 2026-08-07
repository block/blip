module blip_plugin

go 1.24

replace github.com/cashapp/blip/v2 => ../../

require github.com/cashapp/blip/v2 v2.0.1

require (
	github.com/aws/aws-sdk-go-v2 v1.20.3 // indirect
	github.com/aws/smithy-go v1.14.2 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
