---
---

Blip uses [events](https://pkg.go.dev/github.com/cashapp/blip/v2/event) instead of traditional logging.

Instead of,

```go
log.Println("Error connecting to MySQL")
```

Blip emits an event like,

```go
event.Errorf(event.LOST_MYSQL, "Cannot connect to MySQL")
```

Events are either "info" or errors.
By default, error events are printed to `STDERR`.
Start `blip` with the [`--log`]({{< ref "/config/blip#--log" >}}) option to print info events to `STDOUT`, which simulates traditional logging.

An event receiver handles every event.
The default event receiver is [`event.Log`](https://pkg.go.dev/github.com/cashapp/blip/v2/event#Log), which prints events as noted above: error events to `STDERR`, and "info" events to `STDOUT` if [`--log`]({{< ref "/config/blip#--log" >}}).

To change (or implement different) Blip logging, implement a custom event receiver.

## Custom Receiver

Implement the [`event.Receiver` interface](https://pkg.go.dev/github.com/cashapp/blip/v2/event#Receiver), then call [`event.SetReceiver`](https://pkg.go.dev/github.com/cashapp/blip/v2/event#SetReceiver) before booting the server.

<p class="note">
Registering a custom receive completely overrides the default receiver.
Be sure your custom receiver handles all events.
</p>
