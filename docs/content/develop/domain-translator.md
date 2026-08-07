---
---

A _domain translator_ renames Blip metrics before reporting.
This allows Blip to collect and structure metrics one way, but report metrics in different ways.

## Sink Translator

[sink/tr.Translator](https://pkg.go.dev/github.com/cashapp/blip/v2/sink/tr#DomainTranslator)

The sink translator renames metrics.
A sink translator must be registered by calling [`sink/tr.Register`](https://pkg.go.dev/github.com/cashapp/blip/v2/sink/tr#Register).

## Prometheus Translator

[prom.Translator](https://pkg.go.dev/github.com/cashapp/blip/v2/prom#DomainTranslator)

The Prometheus translator is used for [Prometheus emulation]({{< ref "config/prometheus" >}}).
Currently, these translators are built-in.
To add or change one, submit a PR.
