# Blip Example: Using Integrations

This example demonstrates using several different to customize Blip:

* Register custom metrics collector
* Set a custom plugin
* Set a custom factory
* Set a variable to a custom value

All of the customizations in this example are stubs&mdash;they don't do anything real or useful. But they demonstrate how to integrate into Blip by using various integration points.

The `main.go` in this directory is similar to the default Blip main in `../../bin/blip/main.go`. The difference is that this custom code integrates into the Blip server before booting and running. Consequently, this `main.go` build a Blip binary with the same Blip core as the default Blip main, but with added integrations. You can see this by compiling and running this `main.go`:

```
% go build

% ./integrate --debug
2022/07/16 16:22:26.077845 DEBUG server.go:101 blip 0.0.0 {Options:{Config: Debug:true Help:false Plans: Log:false PrintConfig:false PrintDomains:false PrintMonitors:false PrintPlans:false Run:true Version:false} Args:[./integrate]}
2022/07/16 16:22:26.077960 [boot-start               ] [] blip 0.0.0
2022/07/16 16:22:26.077969 [boot-config-loading      ] []
2022/07/16 16:22:26.077979 DEBUG server.go:141 call plugins.LoadConfig
2022/07/16 16:22:26.077989 DEBUG main.go:28 plugins.LoadConfig called
```

The last line shows the debug output from the `LoadConfig` plugin. It's the same Blip, but with added integrations.

This example registers a custom metrics collector for a custom domain called "foo". That makes the following plan valid:

```yaml
level:
  collect:
    foo:
      # Handled by FooMetrics
```
