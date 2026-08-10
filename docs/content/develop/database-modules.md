---
---

Blip is purpose-built for MySQL. An external database module can reuse its monitor, plan, collection, transformation, and sink runtime without adding that database implementation to Blip itself.

{{< toc >}}

## Enable before boot

The integrating binary must activate an external module before `Server.Boot`. A module should provide one entry point. It must:

1. Register its database type with `blip.RegisterDatabaseModule`.
2. Register its collectors through the existing `metrics.Register` API.
3. Decorate `Factories.DbConn` with its connection factory.

The integrating binary passes the values returned by `server.Defaults` to the module before boot:

```go
env, plugins, factories := server.Defaults()
if err := mymodule.Enable(plugins, &factories); err != nil {
    log.Fatal(err)
}

s := server.Server{}
if err := s.Boot(env, plugins, factories); err != nil {
    log.Fatal(err)
}
```

`server.Defaults` leaves `Factories.DbConn` unset because `Server.Boot` normally constructs Blip's built-in MySQL factory after loading configuration. A module that decorates the factory before boot must preserve that behavior. If `Factories.DbConn` is nil, construct the default MySQL factory with the configured AWS factory, `ModifyDB` plugin, and password-secret parser before wrapping it:

```go
fallback := factories.DbConn
if fallback == nil {
    fallback = dbconn.NewConnFactory(
        factories.AWSConfig,
        plugins.ModifyDB,
        dbconn.WithPasswordSecretParser(plugins.ParsePasswordSecret),
    )
}
factories.DbConn = NewDatabaseFactory(fallback, moduleFactory)
```

The module's connection factory handles only its own database type and delegates every other type to the previous factory. If the decorator implements `DbProviderFactory`, its fallback path must call the previous factory's `MakeProvider` when available. Otherwise, it must call the previous `Make` method and wrap that connection in a single-pool provider whose `Close` method closes the connection. This convention preserves legacy factories and allows multiple external modules to compose in any activation order.

If module activation stops after registering the database type or any collectors, remove those registrations before returning the error. This rollback allows a corrected activation attempt in the same process.

## Configuration

Every external monitor sets [`database-type`]({{< ref "/config/config-file#database-type" >}}) and can provide an opaque [`database-config`]({{< ref "/config/config-file#database-config" >}}) map:

```yaml
monitors:
  - database-type: my-database
    hostname: database.example:1234
    username: metrics
    database-config:
      module-option: value
```

An omitted database type remains MySQL. `database-type` supports direct `${ENV_VAR}` interpolation but not monitor-field interpolation because it selects defaults before the rest of monitor initialization.

Blip recursively interpolates string values inside `database-config` and redacts the entire opaque map when logging monitor configuration. A module uses `blip.DecodeDatabaseConfig` to strictly decode the map into its own typed configuration, then applies and validates its defaults in module code.

`DatabaseModule.ValidateConfig` receives monitor configuration by value. It can report configuration problems during monitor loading, but it cannot persist resolved defaults in Blip's opaque map. Use the same decode, default, and validation function from both `ValidateConfig` and the module's connection factory.

External monitors do not inherit the top-level `mysql` defaults or defaults for the MySQL-only exporter, heartbeat, and plan-changing features. They can inherit shared AWS, TLS, tag, sink, and plan-file settings. MySQL socket, `my.cnf`, heartbeat, plan changing, plan-table storage, and `mysqld_exporter` emulation are rejected when configured on an external monitor.

## Connections and credentials

`DbProviderFactory` is optional. A module that needs multiple connection pools returns a `DbProvider`; Blip uses `Primary` for ordinary collectors and closes the provider only after monitor subsystems and collectors stop. A specialized collector factory implements `CollectorFactoryWithDBProvider` and type-asserts the generic provider to a module-owned extension interface.

The shared `credentials.Factory` supports IAM, Secrets Manager, password files, static passwords, and passwordless authentication. Its `Dynamic` method takes the engine's default port explicitly. The module remains responsible for endpoint normalization, credential caching and refresh, authentication-error classification, and connection retry behavior.

## Collector compatibility

A module collector implements `CollectorFactoryDatabaseTypes` and returns its database type. A collector that is independent of the monitor database returns `DatabaseTypeAny`. A factory that does not implement the optional interface retains Blip's historical MySQL compatibility.

Blip validates that database-specific collectors in one plan have a common database type, and it validates the selected plan against each monitor before collector preparation. Database-neutral collectors do not constrain the plan.
