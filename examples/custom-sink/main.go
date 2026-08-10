package main

import (
	"context"
	"fmt"
	"log"

	"gopkg.in/yaml.v2"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/server"
	"github.com/cashapp/blip/v2/sink"
)

type YAML struct{}

var _ blip.Sink = YAML{}
var _ blip.SinkFactory = YAML{}

func (y YAML) Send(ctx context.Context, m *blip.Metrics) error {
	bytes, err := yaml.Marshal(m)
	if err != nil {
		return err
	}
	fmt.Println(string(bytes))
	return nil
}

func (y YAML) Name() string {
	return "yaml"
}

func (y YAML) Make(_ blip.SinkFactoryArgs) (blip.Sink, error) {
	return YAML{}, nil
}

func main() {
	sink.Register("yaml", YAML{}) // Register custom "yaml" sink
	sink.Default = "yaml"         // Change default sink to ^

	// Create, boot, and run the custom Blip server
	s := server.Server{}
	if err := s.Boot(server.Defaults()); err != nil {
		log.Fatalf("server.Boot failed: %s", err)
	}
	if err := s.Run(server.ControlChans()); err != nil { // blocking
		log.Fatalf("server.Run failed: %s", err)
	}
}
