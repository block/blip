---
---

Blip is purpose-built for MySQL. An external database module can reuse its
monitor, plan, collection, transformation, and sink runtime without adding that
database implementation to Blip itself.

{{< toc >}}

## Enable Before Boot

An external module is enabled by the integrating binary before `server.Boot`.
The module must:

1. Register its database type with `blip.RegisterDatabaseModule`.
2. Register its collectors through the existing `metrics.Register` API.
3. Decorate `Factories.DbConn` with its connection factory.

The module's connection factory should handle only its own database type and
delegate every other type to the previous factory. It must also preserve
`DbProviderFactory` delegation when the previous factory implements that
optional capability. This convention allows multiple external modules to
compose without changing Blip's built-in MySQL factory.

## Configuration

Every external monitor sets a literal `database-type` and can provide an opaque
`database-config` map:

```yaml
monitors:
  - database-type: my-database
    hostname: database.example:1234
    username: metrics
    database-config:
      module-option: value
```

An omitted database type remains MySQL. Direct `${ENV_VAR}` interpolation is
supported in `database-type`; monitor-field interpolation is intentionally not
supported because the type selects defaults before the rest of monitor
initialization.

Blip recursively interpolates string values inside `database-config` and
redacts the entire opaque map when logging monitor configuration. A module uses
`blip.DecodeDatabaseConfig` to strictly decode the map into its own typed
configuration, then applies and validates its defaults in module code. Its
`DatabaseModule.ValidateConfig` implementation provides early validation during
monitor loading.

MySQL socket, `my.cnf`, heartbeat, plan changing, plan-table storage, and
`mysqld_exporter` emulation are rejected for external database monitors.

## Connections and Credentials

`DbProviderFactory` is optional. A module that needs multiple connection pools
returns a `DbProvider`; Blip uses `Primary` for ordinary collectors and closes
the provider only after monitor subsystems and collectors stop. A specialized
collector factory implements `CollectorFactoryWithDBProvider` and type-asserts
the generic provider to a module-owned extension interface.

The shared `credentials.Factory` supports IAM, Secrets Manager, password files,
static passwords, and passwordless authentication. Its `Dynamic` method takes
the engine's default port explicitly. The module remains responsible for
endpoint normalization, credential caching and refresh, authentication-error
classification, and connection retry behavior.

## Collector Compatibility

A module collector implements `CollectorFactoryDatabaseTypes` and returns its
database type. A collector that is independent of the monitor database returns
`DatabaseTypeAny`. A factory that does not implement the optional interface
retains Blip's historical MySQL compatibility.

Blip validates that database-specific collectors in one plan have a common
database type, and it validates the selected plan against each monitor before
collector preparation. Database-neutral collectors do not constrain the plan.
