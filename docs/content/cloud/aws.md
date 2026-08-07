---
title: "AWS"
---

{{< hint type=important >}}
Blip works with AWS, but AWS does not support or contribute to Blip.
{{< /hint >}}

Blip has built-in support for Amazon RDS for MySQL and Amazon Aurora.
The following documentation presumes proficiency with AWS.
It does not explain, for example, how to set up IAM roles or configure RDS.

{{< toc >}}

## Region

By default, Blip tries to auto-detect the region by querying [IMDB](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html).
This works if running on an EC2 instance.

If auto-detection fails or [`config.aws.region`]({{< ref "/config/config-file#region" >}}) is set, the value of this variable is used.

If that variable is not set, Blip tries to auto-detect the region from the MySQL endpoint by splitting the fields of the DNS address.
This should work because Amazon RDS endpoints have the format `<db-id>.<cluster>.<region>.rds.amazonaws.com`.

If that fails, no region is used.
Not setting the region in Blip might work if [AWS SDK for Go v2](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2) can auto-detect the region.

## Credentials

Currently, Blip relies on the [AWS SDK for Go v2](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2) to detect and set the credentials through its various conventions.
For example, Blip does not currently have a config variable to specify an AWS profile.
The only information Blip passes to the AWS SDK is the region:

```go
config.LoadDefaultConfig(ctx, config.WithRegion("..."))
```

Section [Region](#region) above explains how the region is auto-detected or configured.

Use the [`blip.AWSConfigFactory`](https://pkg.go.dev/github.com/cashapp/blip/v2#AWSConfigFactory) to load a custome AWS configuration (which includes the credentials).
See [Develop / Intergration API]({{< ref "/develop/integration-api" >}}) for more information.

## Authentication

Blip supports AWS IAM auth and fetching the password from Secrets Manager.
Like all MySQL credentials, **Blip automatically [reloads the password]({{< ref "/monitors/mysql-connection#password-reloading" >}}) on "access denied" from MySQL.**
This is especially important for IAM auth since the token is valid only 15 minutes: Blip automatically regenerates a new token.

### IAM Authentication

Blip can use an IAM authentication token as the password:

* [IAM database authentication for MariaDB, MySQL, and PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html)
* [How do I allow users to authenticate to an Amazon RDS MySQL DB instance using their IAM credentials?](https://aws.amazon.com/premiumsupport/knowledge-center/users-connect-rds-iam/)

IAM auth must be enabled on the RDS instance first.
To check, execute:

```sql
SELECT * FROM INFORMATION_SCHEMA.PLUGINS WHERE plugin_name='AWSAuthenticationPlugin'\G

*************************** 1. row ***************************
           PLUGIN_NAME: AWSAuthenticationPlugin
        PLUGIN_VERSION: 1.0
         PLUGIN_STATUS: ACTIVE
           PLUGIN_TYPE: AUTHENTICATION
   PLUGIN_TYPE_VERSION: 2.0
        PLUGIN_LIBRARY: aws_auth.so
PLUGIN_LIBRARY_VERSION: 1.10
         PLUGIN_AUTHOR: AWSRDS
    PLUGIN_DESCRIPTION: Aurora AWS Authentication Plugin
        PLUGIN_LICENSE: PROPRIETARY
           LOAD_OPTION: ON
```

The output above means IAM is enabled on the instance.

Once IAM auth is enable, first create the Blip user as documented by AWS:

```sql
CREATE USER 'blip' IDENTIFIED WITH AWSAuthenticationPlugin as 'RDS' REQUIRE SSL;
GRANT SELECT, PROCESS, REPLICATION CLIENT ON *.* TO 'blip'@'%';
```

Then you need an IAM policy that allows the `blip` MySQL user to connect using IAM auth:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowBlipToConnect",
            "Effect": "Allow",
            "Action": "rds-db:connect",
            "Resource": "arn:aws:rds-db:*:999999999999:dbuser:*/blip"
        }
    ]
}
```

Replace `999999999999` with your AWS account ID.

Then set [`config.aws.iam-auth`]({{< ref "/config/config-file#iam-auth" >}}) to true and start Blip.
If everything is configured correctly in both Blip and AWS, Blip should work as usual.

### Secrets Manager

Blip can fetch its MySQL password from a secret in [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/).
Set [`config.aws.password-secret`]({{< ref "/config/config-file#password-secret" >}}) to the ARN of the secret.
By default, the secret value must be a JSON object with a string `password` key, like:

```json
{
  "username": "blip",
  "password": "...",
  "engine": "mysql",
  "host": "db.cluster.us-east-1.rds.amazonaws.com",
  "port": 3306,
  "dbClusterIdentifier": "db"
}
```

By default, Blip ignores other keys in the secret value and reads only `username` and `password`.
The `username` key is optional; if it is not set, Blip uses the configured monitor username.

{{< hint type=note >}}
The example above is the default secret map that AWS creates for RDS.
It works with Blip, but Blip ignores the connection fields like `host` and `port`.
{{< /hint >}}

If you embed Blip and use a different secret payload shape, set the
[`blip.Plugins.ParsePasswordSecret`](https://pkg.go.dev/github.com/cashapp/blip/v2#Plugins)
callback before calling `Server.Boot`.
The callback receives one raw payload: `SecretString` bytes when present, otherwise `SecretBinary` bytes.
It should set the password and, if needed, username on the credentials value passed to it.

The [AWS credentials](#credentials) that Blip uses must be allowed to read the secret with a policy privilege like:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ReadBlipPasswordSecret",
            "Effect": "Allow",
            "Action": "secretsmanager:GetSecretValue",
            "Resource": "arn:aws:secretsmanager:*:999999999999:secret:blip-password-ABCDEF"
        }
    ]
}
```

Replace `999999999999` with your AWS account ID, and replace `blip-password-ABCDEF` with your secret ID.

## TLS

When connecting an Amazon RDS instances (RDS or Aurora), Blip automatically detects and enables TLS using the Amazon RDS global CA, which is hard-coded into Blip at the end of [`blip/aws/rds.go`](https://github.com/cashapp/blip/blob/main/aws/rds.go).

You can disable this by explicitly setting any [`config.tls`]({{< ref "/config/config-file#tls" >}}) variable, or by setting [`config.aws.disable-auto-tls`]({{< ref "/config/config-file#disable-auto-tls" >}}) to true.
