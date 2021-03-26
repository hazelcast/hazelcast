---
title: Version Compatibility
description: Compatibility between different Jet versions
---

The following rules currently apply for Jet for compile time
and runtime compatibility.

## Semantic Versioning

Hazelcast Jet uses [Semantic Versioning](https://semver.org/) which
can be summarized as follows:

* MAJOR version when you make incompatible API changes,
* MINOR version when you add functionality in a backwards-compatible
  manner, and
* PATCH version when you make backwards-compatible bug fixes.

This means that a Jet job written using Pipeline API in a previous
minor version should compile in later minor versions.

However some exceptions apply:

* Classes in `com.hazelcast.jet.core` package which form the only
  provide PATCH level compatibility guarantee.
* Types and methods annotated with `@EvolvingApi` only provide PATCH
  level compatibility guarantee. These are typically new features where
  the API is subject to further changes.
* Classes in `impl` packages do not provide any compatibility guarantees
  between versions and are not meant for public use.

## Summary Table

The compatibility guarantees can be summarized as follows:

|Type|Component|Guarantee
|----|---------|---------|
|Compile Time|Job API|MINOR
|Compile Time|Pipeline API|MINOR
|Compile Time|Core API|PATCH
|Runtime|Member to Member|NONE
|Runtime|Management Center to Member|NONE
|Runtime|Client to Member|PATCH
|Runtime|Job State|PATCH
|Runtime|Command Line Tools|MINOR
|Runtime|Configuration XML files|PATCH
|Runtime|Metrics (JMX)|PATCH

## Runtime Compatibility

### Members

Jet requires that all members in a cluster use the same PATCH version.
When updating Jet to a newer PATCH version, the whole cluster must be
shutdown and restarted with the newer version at once.

### Clients

Jet clients are compatible with the members running on the same MINOR
version. This means that a client using an older or newer PATCH version
should be able to connect and work with a cluster that's running a
different PATCH version.

### Management Center

Prior to 4.3, Jet provides its own management center which can be used
to monitor the Jet cluster and manage the lifecycle of the jobs. The
Management Center, like members, is only compatible with the same PATCH
version. This means that Management Center and the cluster must have the
exact same PATCH version to be compatible.

Starting 4.3, Jet Management Center merged to Hazelcast Management
Center. Compatible versions are listed below:

| Jet | Management Center |
|-----|-------------------|
| 4.2 | 4.2020.08 |
| 4.3 | 4.2020.10 |

## Job State Compatibility

Job state is only compatible across the same MINOR version and only
backwards-compatible i.e. a newer PATCH version is be able to understand
the job state from a previous PATCH version.

This means that if you have a running job, using the Job Upgrade and
Lossless Cluster Restart features you are able to upgrade the cluster to
a newer PATCH version without losing the state of a running job.

## Command Line Tools and Configuration Files

The command line tools provided such as `jet` and the configuration XML
files are backwards-compatible between MINOR versions. This means that
when upgrading a cluster to a new minor version, the XML configuration
for the previous version can be used without any modification.

## Metrics

Jet currently provides metrics to Management Center and also through
other means such as JMX. The metrics names may change between MINOR
versions but not between PATCH versions.
