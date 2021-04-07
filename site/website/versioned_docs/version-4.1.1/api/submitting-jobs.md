---
title: Submitting Jobs
description: How to submit jobs to a Jet cluster.
id: version-4.1.1-submitting-jobs
original_id: submitting-jobs
---

To execute a Jet job it needs to be submitted to the cluster. Once the
job is submitted, it's distributed automatically to all the cluster members
and will be executed on all members by default. Jet offers
several ways to submit the job.

A typical Jet program typically has the following structure:

* Acquire the `JetInstance` through `Jet.bootstrappedInstance()` or one
  of the other available means
* Create the data pipeline using `Pipeline.create()`
* Add the required transforms to the pipeline
* Submit the pipeline as a job using `JetInstance.newJob()`
* For a batch job, call `Job.join()` to wait for the batch job to
  complete
* For a streaming job, exit the program after `newJob()`, or where
  desired, have a loop to constantly monitor the results. Calling
  `.join()` on a streaming job will not return any results until a job
  is cancelled or fails.

## Submit Job as a JAR

The simplest way to submit a Job to the Jet cluster is to use
the `jet submit` command. When you submit a job
using this command, then the provided JAR itself is sent to the cluster
and all the classes inside the supplied JAR will be available during
execution.

When using the `jet submit` command, you need to acquire a
`JetInstance` using `Jet.bootstrappedInstance()` instead of other
options (i.e. do not use `Jet.newJetClient()`).

The `jet submit` command by default will execute the main class of the
application, but you can optionally specify which class should run:

```bash
bin/jet submit --class MainClass <jar file> [<arguments>...]
```

The command also has some additional options:

* `-v`: for verbose mode, which will show the connection logs and
  exception stack traces, if any
* `-n`: job name to use, will override the one set in `JobConfig`
* `-s`: name of the initial snapshot to start the job from

The `jet` command also some global options:

* `-a`: comma separated addresses for the cluster
* `-n`: cluster name, which must match what's configured on the cluster
* `-f`: path to a client config file, which can be used instead of
  supplying an address and name.

Example:

```bash
bin/jet \
  -a 192.168.1.100:5701,192.168.1.101:5701 \
  -n jet \
  submit \
  -c <MainClass> \
  <jar file> \
  [<arguments>...]
```

## Bundling Dependencies

A Jet pipeline is built with several transform which typically consist
of lambda expressions. During the job submission, the pipeline is
[serialized](serialization) and sent to the Jet cluster, which must be
able to execute these expressions on each node. Imagine the simple
mapping pipeline below:

```java
class JetApp {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    p.readFrom(TestSources.items(1, 2, 3, 4))
     .map(x -> x * x)
     .writeTo(Sinks.logger());

     JetInstance jet = Jet.bootstrappedInstance();
     jet.newJob(p).join();
  }
}
```

The lambda `x -> x * x` will get compiled by Java into an anonymous
class with a name like `JetApp$$Lambda$30/0x00000008000a1840`. These and
other classes which may be depend by these functions need to present
on the nodes that will be executing the job. Jet supports several ways
to make these classes available on the nodes.

### Uber JAR

The easiest way to get additional dependencies to the cluster is to
bundle it as a so-called uber JAR, which contains all the required
dependencies inside.

To build an uber JAR, there are several options:

* [Maven Assembly Plugin](https://maven.apache.org/plugins/maven-assembly-plugin/)
* [Maven Shade Plugin](https://maven.apache.org/plugins/maven-shade-plugin/)
* [Gradle Shadow Plugin](https://imperceptiblethoughts.com/shadow/introduction/).

When shading dependencies, `com.hazelcast.jet:hazelcast-jet` itself
doesn't need to be inside the JAR and should be set with the `provided`
scope or equivalent.

### Adding to Classpath

Some dependencies may either be large or may be required to be present
on classpath during application startup.

The convention is to add these dependencies to `<JET_HOME>/lib` folder.
Jet automatically picks up any dependencies placed on this folder during
startup. Several of Jet's out of the box modules (such as connectors for
Kafka, Hadoop) are already available in the `opt` folder and can simply
be moved to `lib` folder to be used. Any changes to `lib` folder
requires the node to be restarted to take effect.

Alternatively, you can use the `CLASSPATH` environment variable
to add additional classes:

```bash
CLASSPATH=<path_to_jar> bin/jet-start
```

There are some specific types of classes which are required to be
present on the classpath and can't be dynamically sent along with the
Job. These include:

* [Custom Serializers](https://jet-start.sh/docs/api/serialization#serialization-of-data-types)
* Some [`IMap`](data-structures#imap) specific features like
  `EntryProcessor` or `MapLoader`/`MapStore`

## Submitting using Jet Client

If you are using a Jet client as part of an application and need to
submit jobs within that application then you can also use the Jet client
to directly submit jobs:

```java
JetInstance jet = Jet.newJetClient();
..
jet.newJob(pipeline);
```

### Attaching Specific Classes

When submitting a Job using the Jet client, no additional classes are
sent with the job by default, and will need to be manually added:

```java
JetInstance jet = Jet.newJetClient();
jet.newJob(pipeline, new JobConfig().addClass(AppClass.class))
```

When adding classes this way, nested (inner & anonymous) classes are
automatically added as well.

It's also possible to add all the classes in a given package (and
recursively in all of its subpackages) using `JobConfig.addPackage` or
even a whole JAR with `JobConfig.addJar`.

## Setting the Job Name

Each job has a cluster-wide unique ID and an optional name. Only one
job with the same name can be running in the cluster at the same time.
You can configure the job name through `JobConfig.setName()` option.

When a job is already running with the same name, the newly submitted
one will fail. You can avoid this by using the
`JetInstance.newJobIfAbsent()` method.

## Setting Processing Guarantees

When the job is submitted it's possible to set what fault-tolerance
level should the job support. There are three options:

* `NONE`: No snapshots will be taken and upon a restart due to cluster
  change, the job will be restarted as if it was started from scratch.
* `AT_LEAST_ONCE`: Enables at-least-once guarantee. Regular snapshots
  will be taken and the job can be resumed from these snapshots when
  restarted.
* `EXACTLY_ONCE`: Enables exactly-once guarantee. Regular snapshots will
  be taken and the job can be resumed from these snapshots when
  restarted.

For a more detailed discussion on these guarantees, please refer to the
[Fault Tolerance](../architecture/fault-tolerance) section.
The snapshot interval can be configured by `JobConfig.setSnapshotIntervalMillis()`.
The default is 10 seconds.
