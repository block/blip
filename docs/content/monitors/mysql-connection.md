---
title: "MySQL Connection"
---

Blip can connect to MySQL in every way possible, including high-security options like _client_ authentication with private certificates.
Once running, Blip tries forever to connect (or reconnect) to MySQL.

{{< toc >}}

## my.cnf

Blip can read the `[client]` section of a MySQL defaults file like `/etc/my.cnf` or `~/.my.cnf` by specifying [`config.mysql.mycnf`]({{< ref "/config/config-file#mycnf" >}}).
Like MySQL, a defaults file specifies _defaults_ that overwritten by other, more explicit configuration.
For example, if the MySQL username is specified in a defaults file and [`config.mysql.username`]({{< ref "/config/config-file#username" >}}), the latter is used.

The following variables are read from a MySQL defaults file:

|my.cnf Variable|Blip Variable|
|---------------|-------------|
|`host`|[`config.mysql.hostname`]({{< ref "/config/config-file#hostname" >}})|
|`password`|[`config.mysql.password`]({{< ref "/config/config-file#password" >}})|
|`port`|(Appended to Blip DSN)|
|`socket`|[`config.mysql.socket`]({{< ref "/config/config-file#socket" >}})|
|`ssl-mode`|(See below)|
|`ssl-ca`|[`config.tls.ca`]({{< ref "/config/config-file#ca" >}})|
|`ssl-cert`|[`config.tls.cert`]({{< ref "/config/config-file#cert" >}})|
|`ssl-key`|[`config.tls.key`]({{< ref "/config/config-file#key" >}})|
|`user`|[`config.mysql.username`]({{< ref "/config/config-file#username" >}})|

MySQL `ssl-mode=DISABLED` disables Blip TLS even if other TLS variables are set.
`ssl-mode=PREFERRED` is used only if a socket is not used.
To use TLS with a socket, set `ssl-mode=REQUIRED` as per the MySQL manual.

## Authentication

### Methods

Blip supports the authentication methods listed below.
Although you can configure different methods, Blip uses only one method to connect to MySQL.
If multiple are configured, the order of precedence is:

1. AWS IAM authentication ([`config.aws.iam-auth`]({{< ref "/config/config-file#iam-auth" >}}))
1. AWS Secrets Manager ([`config.aws.password-secret`]({{< ref "/config/config-file#password-secret" >}}))
1. Password file ([`config.mysql.password-file`]({{< ref "/config/config-file#password-file" >}}))
1. my.cnf ([`config.mysql.mycnf`]({{< ref "/config/config-file#mycnf" >}}))
1. Password ([`config.mysql.password`]({{< ref "/config/config-file#password" >}}))
1. No password

### TLS Client Authentication

TLS client authentication occurs (or is required) when the MySQL user is created to require it.
See [`CREATE USER`](https://dev.mysql.com/doc/refman/en/create-user.html) in the MySQL manual.

Once the MySQL user is created to require TLS authentication, set [`config.tls`]({{< ref "/config/config-file#tls" >}}) in the Blip config file or a [my.cnf]({{< ref "#mycnf" >}}) file, and do not set any password.

### Password Reloading

Blip uses [go-mysql/hotswap-dsn-driver](https://github.com/go-mysql/hotswap-dsn-driver) to automatically reload the password (and TLS certificates, if any) for _all_ authentication methods.
This occurs any time MySQL returns error 1045: access denied.
Currently, this cannot be disabled.

## Limits

Blip is limited to 3 connections per monitor.
This can be changed by using the [`blip.ModifyDB` plugin](https://pkg.go.dev/github.com/cashapp/blip/v2#Plugins), but this is not advised.

Blip collects metrics in parallel with a limit of 2 collectors (domains) at once.
This can be changed by setting the [`monitor.CollectParallel` variable](https://pkg.go.dev/github.com/cashapp/blip/v2/monitor#pkg-variables), but this is not advised.

The 3 connection limit minus 2 parallel metrics collection leaves 1 connection free that is used by the [Heartbeat]({{< ref "/heartbeat" >}}) and [Plan Changer]({{< ref "/plans/changing" >}}).
