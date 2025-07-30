# Contributing to Blip

Contributions are encouraged and welcomed.

Blip was designed to be developed and improved by the community because MySQL monitoring is too extensive and dynamic for one team or company to handle.
For Blip to remain the best and most advanced MySQL monitor, contributions are not only welcomes, they're required.

## Contributor License Agreement

Before Block (Square/Cash App) can accept your code, you must sign the [Individual Contributor License Agreement (CLA)](https://docs.google.com/forms/d/e/1FAIpQLSeRVQ35-gq2vdSxD1kdh7CJwRdjmUA0EZ9gRXaWYoUeKPZEQQ/viewform?formkey=dDViT2xzUHAwRkI3X3k5Z0lQM091OGc6MQ&ndplr=1).
If you already signed it for other Block (Square/Cash App) projects, let us know.
It only needs to be signed once.

## Core Developers and Maintainers

Currently, the core developers and maintainers (CDM) are [Block](https://block.xyz) engineers.

"We" and "us" in this document refers to the CDM.

## Contributing

Contributing to this project follows the standard flow that you've probably done a thousand times:

1. Create an issue
2. Create a PR
3. Wait for review and approval
4. Merge and delete your branch

There is no general or specific turnaround time or ETA on contributions because it depends on the issue: some are quick and easy; others are unclear.

### Bug Fixes

Bug fixes are top priority because a monitor must be more reliable than what it monitors.
Since MySQL is extremely reliable, the bar for Blip is very high.

For bug reports and fixes, please include your:

* Blip version
* Config
* Plans
* All output including `--debug`
* MySQL distributions and versions
* Relevant details like "happens every day at 03:33 UTC"

:warning: Config and `--debug` out might contain passwords! Blip tries to redact all passwords, but check your output before submitting.

### New Features

Before coding new features, please create an issue and discuss you ideas before writing to ensure that we are aligned on the direction and outcome.

As an open source project intended for the public&mdash;and already running on thousands of MySQL instances&mdash;we must be judicious in the develop of Blip to ensure that works for all MySQL.

Although Blip was created at Block, we welcome new features that Block does not need because we know that improving Blip for others improves it for all.

### Documentation

We really appreciate contributions to the docs because, as engineers too, we understand how difficult it is to maintain the docs.
To ensure the quality and clarity of the docs for all readers, please expect that we will copyedit your contributions.

If you want to make large changes to the docs, please create an issue and discuss you ideas before writing to ensure that we are aligned on the direction and outcome.

## Code Standards

At minimum, all code contributions must meet these criteria:

- [x] Passes all tests
- [x] Has good code comments
- [x] Updates the relevant [docs](https://block.github.io/blip/)
- [x] Is formatted with `go fmt`
- [x] Is idiomatic Go and well designed
- [x] Follows existing conventions and style

Since a monitor must be more reliable than what it monitors, please expect that we will hold all code contributions to a high standard.

## Versioning

Blip follows [semantic versioning](https://semver.org/) with a few allowances noted below.
As engineers who operate a large fleet of MySQL too, we understand the value of semantic versioning for daily operations.

* **Patch-level** version changes are guaranteed to be backward-compatible, drop-in upgrades.
_For daily operations, you can pin the major.minor version and safely upgrade to any patch-level release._
<br><br>New config, collector, and sink options might be allowed in a patch-level change as long as they are fully backwards-compatible.
For example: adding an option to an existing metrics collector that is off by default and affects nothing else.
<br><br>

* **Minor** version changes add new features and should be backward-compatible, but we will explicitly call out in the release notes any minor changes that might require updating your config, collector, or sink options.
For example: a new minor version might change some default behavior that may or may not be backwards-compatible depending on one's specific usage.
Or for example: a new minor version might modify a Blip table, requiring the user to run an `ALTER`.
In general, we strive to make minor version changes backwards-compatible.
<br><br>

* **Major** version changes introduce new public APIs, especially with respect to developer integrations: plugins, factories, and so forth.
