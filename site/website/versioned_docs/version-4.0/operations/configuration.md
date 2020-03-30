---
title: Configuration
description: How to configure Jet for various use cases.
id: version-4.0-configuration
original_id: configuration
---

Hazelcast Jet offers different ways to configure it depending on which
context you want to use Jet in.

## Configuration Files

Inside the Hazelcast Jet home folder you will find a few different
configuration files:

```text
config/hazelcast-jet.yaml
config/hazelcast.yaml
config/hazelcast-client.yaml
```

`hazelcast-jet.yaml` is used for configuring Jet specific configuration
and `hazelcast.yaml` refers to Hazelcast configuration.

Each Jet node is also a Hazelcast node, and `hazelcast.yaml` includes
the configuration specific to clustering, discovery and so forth.

`hazelcast-client.yaml` refers to a Jet client configuration. This config
file is only used by the `jet` command line client to connect to the cluster,
but you can use it as a template for your own configuration files.

## Client Configuration

When using the Jet client as part of your application, the easiest way to
configure it using the client API:

```java
JetClientConfig config = new JetClientConfig();
config.getNetworkConfig().addAddress("server1", "server2:5702");
JetInstance jet = Jet.newJetClient(config);
```

Alternatively, you can add `hazelcast-client.yaml` to the classpath or
working directly which will be picked up automatically. The location of
the file can also be given using the `hazelcast.client.config` system
property.

A sample client YAML file is given below:

```yaml
hazelcast-client:
  # Name of the cluster to connect to. Must match the name configured on the
  # cluster members.
  cluster-name: jet
  network:
    # List of addresses for the client to try to connect to. All members of
    # a Hazelcast Jet cluster accept client connections.
    cluster-members:
      - server1:5701
      - server2:5701
  connection-strategy:
    connection-retry:
      # how long the client should keep trying connecting to the server
      cluster-connect-timeout-millis: 3000
```

## Configuration for embedded mode

When you are using an embedded Jet node, the easiest way to pass configuration
is to use programmatic configuration:

```java
JetConfig jetConfig = new JetConfig();
jetConfig.getInstanceConfig().setCooperativeThreadCount(4);
jetConfig.configureHazelcast(c -> {
    c.getNetworkConfig().setPort(5000);
});
JetInstance jet = Jet.newJetInstance(jetConfig);
```

Alternatively, you can configure Jet to load its configuration from the
classpath or working directory. By default it will search for
`hazelcast-jet.yaml` and `hazelcast.yaml` files in the classpath and
working directory, but you can control the name of the files using the
relevant system properties, `hazelcast.jet.config` and
`hazelcast.config`, respectively.

## Job-specific Configuration

Each Jet job also has job-specific configuration options. These are covered
in detail under [Job Management](job-management)

## List of configuration options

The files `config/examples/hazelcast-jet-full-example.yaml`  and
`config/examples/hazelcast-full-example.yaml` include  a description of
all configuration options. Jet specific configuration options are listed
below:

|Option|Default|Description|
|------|:------|:----|
|instance/cooperativeThreadCount|number of cores|The number of threads Jet creates in its cooperative multithreading pool. |
|instance/setFlowControlPeriodMs|100|Jet uses a flow control mechanism between cluster members to prevent a slower vertex from getting overflowed with data from a faster upstream vertex. Each receiver regularly reports to each sender how much more data it may send over a given DAG edge. This method sets the duration (in milliseconds) of the interval between flow-control packets.|
|instance/setBackupCount|1|The number of synchronous backups to configure on the IMap that Jet needs internally to store job metadata and snapshots. The maximum allowed value is 6.|
|instance/setScaleUpDelayMillis|10,000|The delay after which the auto-scaled jobs restart if a new member joins the cluster. It has no effect on jobs with auto scaling disabled.|
|instance/setLosslessRestartEnabled|false|Specifies whether the Lossless Cluster Restart feature is enabled. With this feature, you can restart the whole cluster without losing the jobs and their state. It is implemented on top of Hazelcast IMDG's Hot Restart Persistence feature, which persists the data to disk. You need to have the Hazelcast Jet Enterprise edition and configure Hazelcast IMDG's Hot Restart Persistence to use this feature. The default value is `false`, i.e., disabled.|

## List of configuration properties

Configuration properties can either be configured through Java system
properties (specified using the standard `-Dproperty=value`) syntax
before application startup or under the `properties:` inside the yaml
file:

```yaml
hazelcast-jet:
  properties:
    jet.idle.cooperative.min.microseconds: 50
    jet.idle.cooperative.max.microseconds: 500
    jet.idle.noncooperative.min.microseconds: 50
    jet.idle.noncooperative.max.microseconds: 1000
```

You can also configure Jet before starting as follows:

```bash
JAVA_OPTS=-D<property>=<value> bin/jet-start
```

The full list of Jet-specific properties can be found inside the
`com.hazelcast.jet.core.JetProperties` class and the rest of properties
are located inside `com.hazelcast.spi.properties.ClusterProperty` class.
The most important properties are listed here:

|Option|Default|Description|
|------|:------|----------|
|hazelcast.partition.count|271|Total number of partitions in the cluster.|
|hazelcast.logging.type|jdk|What logger should be used by Jet. Valid options are `log4j`, `log4j2`, `slf4j` and `none`|
|jet.idle.cooperative.min.microseconds|25|The minimum time in microseconds the cooperative worker threads will sleep if none of the tasklets made any progress. Lower values increase idle CPU usage but may result in decreased latency. Higher values will increase latency and very high values (>10000µs) will also limit throughput.|
|jet.idle.cooperative.max.microseconds`|500|The maximum time in microseconds the cooperative worker threads will sleep if none of the tasklets made any progress. Lower values increase idle CPU usage but may result in decreased latency. Higher values will increase latency and very high values (>10000µs) will also limit throughput.|
|jet.idle.noncooperative.min.microseconds|25|The minimum time in microseconds the non-cooperative worker threads will sleep if none of the tasklets made any progress. Lower values increase idle CPU usage but may result in decreased latency. Higher values will increase latency and very high values (>10000µs) will also limit throughput.|
|jet.idle.noncooperative.max.microseconds|5000|The maximum time in microseconds the non-cooperative worker threads will sleep if none of the tasklets made any progress. Lower values increase idle CPU usage but may result in decreased latency. Higher values will increase latency and very high values (>10000µs) will also limit throughput.|
|jet.job.results.max.size|1000|Maximum number of job results to keep in the cluster, the oldest results will be automatically deleted after this size is reached.|
|jet.job.results.ttl.seconds|604800|Maximum number of time in seconds the job results will be kept in the cluster. They will be automatically deleted after this period is reached.|
