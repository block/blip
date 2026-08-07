---
weight: 100
---

The following is a _quick references_, not a complete or valid example.
See [Config File]({{< ref "config-file" >}}) for details.

## Interpolation

```
${ENV_VAR}
%{monitor.hostname}
%{monitor.meta.region}
```

## Config File

```yaml
---
# ---------------------------------------------------------------------------
# Server config
# ---------------------------------------------------------------------------

api:
  bind: 127.0.0.1:7522
  disable: false

http:
  proxy: "http://proxy.internal"

monitor-loader:
  aws:
    regions: ["auto", "us-east-1"]
  files:
    - "some-mysql.yaml"
    - "more-mysql.yaml"
  local:
    disable-auto: true
    disable-auto-root: true
  stop-loss: 50%

# ---------------------------------------------------------------------------
# Monitor defaults
# ---------------------------------------------------------------------------

aws:
  auth-token: true
  disable-auto-region: false
  disable-auto-tls: false
  password-secret: "arn::::"
  region: "us-east-1"

exporter:
  flags:
    web.listen-address: "127.0.0.1:9104"
    web.telemetry-path: "/metrics"
  mode: "dual" # or "legacy"

heartbeat:
  freq: 2s
  source-id: "source-host.local"
  role: "west-side"
  table: "blip.heartbeat"

mysql:
  mycnf: "/app/my.cnf"
  password: "..."
  password-file: "/var/shm/blip-passwd"
  socket: "/var/lib/mysql.sock"
  timeout-connect: 5s
  username: "blip"

plans:
  change:
    offline:
      after: 1s
      plan: none.yaml
    standby:
      after: 1s
      plan: none.yaml
    read-only:
      after: 1s
      plan: ro-plan.yaml
    active:
      after: 1s
      plan: active-plan.yaml
  disable-default-plans: false
  files:
    - none.yaml
    - ro-plan.yaml
    - active-plan.yaml
    - special.yaml
  monitor: <monitor>
  table: "blip.plans"

sinks:
  chronosphere:
    # See Sinks > chorosphere
  datadog:
    # See Sinks > datadog
  log:
    # No options
  noop:
    # No options
  retry:
    buffer-size: 60
    send-timeout: 5s
    send-retry-wait: 200ms
  signalfx:
    # See Sinks > signalfx

tags:
  env: ${ENVIRONMENT:-dev}
  dc: ${DATACENTER:-local}
  hostname: "%{monitor.hostname}"

tls:
  ca: "local.ca"
  cert: "/secrets/%{monitor.hostname}.crt"
  disable: false
  key: "/secrets/%{monitor.hostname}.key"
  skip-verify: false

# ---------------------------------------------------------------------------
# Monitors
# ---------------------------------------------------------------------------

monitors:
  # Built-in MySQL monitor
  - id: host1 # Optional; Blip auto-sets based on MySQL config

    # -----------------------------------------------
    # mysql section variables are specified directly:
    hostname: host1.local
    mycnf: my.cnf
    username: metrics
    password: foo
    password-file: /dev/shm/mypasswd
    socket: /tmp/mysql.sock
    timeout-connect: 5s

    # ----------------------------------------------------------------------
    # Use a shared plan from top-level config.plans instead of monitor plans
    plan: "special.yaml"

    # -----------------------------------------------------------
    # Override monitor defaults by specifying a top-level section
    tls:
      ca: new.ca # overrides monitor default 'tls.ca: local.ca'

    # ---------------------------------------------------
    # Meta values unique to monitor (no monitor defaults)
    meta:
      repl-source: source-db.local
      canary: yes

    # ----------------------------------------------
    # Tags override monitor defaults or set new tags
    tags:
      hostname: "host1"  # overrides monitor default
      foo:      "bar"    # new tag

  # -------------------------------------------------------------------------
  # External database module monitor (only when registered before server boot)
  - id: external
    database-type: my-database
    hostname: database.example:1234
    username: metrics
    database-config:
      module-option: value
```
