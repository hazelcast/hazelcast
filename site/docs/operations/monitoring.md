---
title: Monitoring and Metrics
description: Options available for monitoring the health and operations of Jet clusters and jobs.
---

## Logging

By default, a Jet node writes logs into the `logs` folder of the
distribution. The logs are configured to roll over daily. You can change
the logging configuration by modifying `config/log4j.properties`.

### Logging in Client and Embedded Mode

When using Jet through the client or in embedded mode, Jet doesn't
automatically add any dependencies to any logging framework and allows
configuration of which facade the logging should be done through. Jet
supports the following loggers:

* `jdk`: java.util.logging (default)
* `log4j`: Apache Log4j
* `log4j2`: Apache Log4j 2
* `slf4j`: SLF4J
* `none`: no logger (i.e. no logging will be made)

To configure the logging facade to use, you need to set a property
in the configuration file:

```yaml
hazelcast-client:
  properties:
    hazelcast.logging.type: log4j2
```

Alternatively, you can use the system property
`-Dhazelcast.logging.type` to configure the logging framework to use.

### Using a Custom Logger

If you'd like to use your own logging implementation, you can configure
the `hazelcast.logging.class` property with a class which implements the
`com.hazelcast.logging.LoggerFactory` interface.

## Metrics

Jet exposes various metrics to facilitate monitoring of the cluster
state and of running jobs.

Metrics have associated *tags* which describe which object the metric
applies to. The tags for job metrics typically indicate the specific
[DAG vertex](../concepts/dag.md) and processor the metric belongs to.

Each metric instance provided belongs to a particular Jet cluster
member, so different cluster members can have their own versions of the
same metric with different values.

The metric collection runs in regular intervals on each member, but note
that the metric collection on different cluster members happens at
different moments in time. So if you try to correlate metrics from
different members, they can be from different moments of time.

There are two broad categories of Jet metrics. For clarity we will group
them based on significant tags which define their granularity.

Last but not least let’s not forget about the fact that each Jet member
is also a [Hazelcast](https://github.com/hazelcast/hazelcast) member, so
Jet also exposes all the metrics available in Hazelcast too.

Let’s look at these 3 broad categories of metrics in detail.

### Hazelcast Metrics

There is a wide range of metrics and statistics provided by Hazelcast:

* statistics of distributed data structures (see [Reference Manual](https://docs.hazelcast.org/docs/{imdg-version}/manual/html-single/index.html#getting-member-statistics))
* executor statistics (see [Reference Manual](https://docs.hazelcast.org/docs/{imdg-version}/manual/html-single/index.html#executor-statistics))
* partition related statistics (state, migration, replication)
* garbage collection statistics
* memory statistics for the JVM which current IMDG member belongs to
  (total physical/free OS memory, max/committed/used/free heap memory
  and max/committed/used/free native memory)
* network traffic related statistics (traffic and queue sizes)
* class loading related statistics
* thread count information (current, peak and daemon thread counts)

### Cluster-wide Metrics

<table class="tg">
  <tr>
    <th>Names</th>
    <th>Description</th>
    <th>Tags</th>
  </tr>
  <tr>
    <td><b>blockingWorkerCount</b></td>
    <td>Number of non-cooperative workers employed.</td>
    <td rowspan="6">
        <i>none</i><br>
        <br>
        Each Jet member will have one instance of this metric.
    </td>
  </tr>
  <tr>
    <td><b>jobs.submitted</b></td>
    <td>Number of computational jobs submitted.</td>
  </tr>
  <tr>
    <td><b>jobs.completedSuccessfully</b></td>
    <td>Number of computational jobs successfully completed.</td>
  </tr>
  <tr>
    <td><b>jobs.completedWithFailure</b></td>
    <td>Number of computational jobs that have failed.</td>
  </tr>
  <tr>
    <td><b>jobs.executionStarted</b></td>
    <td>
        Number of computational job executions started. Each job can
        execute multiple times, for example when it’s restarted or
        suspended and then resumed.
    </td>
  </tr>
  <tr>
    <td><b>jobs.executionTerminated</b></td>
    <td>
        Number of computational job executions finished. Each job can
        execute multiple times, for example when it’s restarted or
        suspended and then resumed.
    </td>
  </tr>
  <tr><td colspan="3" class="divider"></td></tr>
  <tr>
    <td><b>iterationCount</b></td>
    <td>
        The total number of iterations the driver of tasklets in
        cooperative thread N made. It should increase by at least 250
        iterations/s. Lower value means some of the cooperative
        processors blocks for too long. Somewhat lower value is normal
        if there are many tasklets assigned to the processor. Lower
        value affects the latency.
    </td>
    <td rowspan="2">
        <b>cooperativeWorker</b><br>
        <br>
        Each Jet member will have one of this metric for each of its
        cooperative worker threads.
    </td>
  </tr>
  <tr>
    <td><b>taskletCount</b></td>
    <td>The number of assigned tasklets to cooperative thread N.</td>
  </tr>
</table>

### Job-specific Metrics

All job specific metrics have their `job=<jobId>` and
`exec=<executionId>` tags set and most also have the
`vertex=<vertexName>` tag set (with very few exceptions). This means
that most of these metrics will have at least one instance for each
vertex of each current job execution.

Additionally, if the vertex sourcing them is a data source or data sink,
then the `source` or `sink` tags will also be set to true.

<table>
  <tr>
    <th>Names</th>
    <th>Description</th>
    <th>Tags</th>
  </tr>
  
  <tr>
    <td><b>executionStartTime</b></td>
    <td>
        Start time of the current execution of the job (epoch time in
        milliseconds).
    </td>
    <td rowspan="2" width="30%">
        <b>job, exec</b><br>
        <br>
        There will be a single instance of these metrics for each job
        execution.
    </td>
  </tr>
  
  <tr>
    <td><b>executionCompletionTime</b></td>
    <td>
        Completion time of the current execution of the job(epoch time
        in milliseconds).
    </td>
  </tr>
  
  <tr><td colspan="3" class="divider"></td></tr>
  
  <tr>
    <td><b>snapshotBytes</b></td>
    <td>Total number of bytes written out in the last snapshot.</td>
    <td rowspan="2">
        <b>job, exec, vertex</b><br>
        <br>
        There will be a single instance of these metrics for each
        vertex.
    </td>
  </tr>
  
  <tr>
    <td><b>snapshotKeys</b></td>
    <td>Total number of keys written out in the last snapshot.</td>
  </tr>
  
  <tr><td colspan="3" class="divider"></td></tr>
  
  <tr>
    <td><b>distributedBytesIn</b></td>
    <td>Total number of bytes received from remote members.</td>
    <td rowspan="4">
        <b>job, exec, vertex, ordinal</b><br>
        <br>
        Each Jet member will have an instance of these metrics for each
        ordinal of each vertex of each job execution.<br>
        <br>
        NOTE: These metrics are only present for distributed edges (ie.
        edges producing network traffic).
    </td>
  </tr>
  
  <tr>
    <td><b>distributedBytesOut</b></td>
    <td>Total number of bytes sent to remote members.</td>
  </tr>
  
  <tr>
    <td><b>distributedItemsIn</b></td>
    <td>Total number of items received from remote members.</td>
  </tr>
  
  <tr>
    <td><b>distributedItemsOut</b></td>
    <td>Total number of items sent to remote members.</td>
  </tr>
  
  <tr><td colspan="3" class="divider"></td></tr>
  
  <tr>
    <td><b>topObservedWm</b></td>
    <td>
        This value is equal to the highest coalescedWm on any input edge
        of this processor.
    </td>
    <td rowspan="7">
        <b>job, exec, vertex, proc</b><br>
        <br>
        Each Jet member will have one instances of these metrics for
        each processor instance N, the N denotes the global processor
        index. Processor is the parallel worker doing the work of the
        vertex.
    </td>
  </tr>
  
  <tr>
    <td><b>coalescedWm</b></td>
    <td>
        The highest watermark received from all inputs that was sent to
        the processor to handle.
    </td>
  </tr>
  
  <tr>
    <td><b>lastForwardedWm</b></td>
    <td>Last watermark emitted by the processor to output.</td>
  </tr>
  
  <tr>
    <td><b>lastForwardedWmLatency</b></td>
    <td>
        The difference between <i>lastForwardedWn</i> and the system
        time at the moment when metrics were collected.
    </td>
  </tr>

  <tr>
    <td><b>queuesCapacity</b></td>
    <td>The total capacity of input queues.</td>
  </tr>
  
  <tr>
    <td><b>queuesSize</b></td>
    <td>The total number of items waiting in input queues.</td>
  </tr>
  
  <tr><td colspan="3" class="divider"></td></tr>
  
  <tr>
    <td><b>topObservedWm</b></td>
    <td>The highest received watermark from any input on edge N.</td>
    <td rowspan="6">
        <b>job, exec, vertex, proc, ordinal</b><br>
        <br>
        Each Jet member will have one instance of these metrics for each
        edge M (input or output) of each processor N. N is the global
        processor index and M is either the ordinal of the edge or has
        the value snapshot for output items written to state snapshot.
    </td>
  </tr>
  
  <tr>
    <td><b>coalescedWm</b></td>
    <td>
        The highest watermark received from all upstream processors on
        edge N.
    </td>
  </tr>

  <tr>
    <td><b>emittedCount</b></td>
    <td>
        The number of emitted items. This number includes watermarks,
        snapshot barriers etc. Unlike <i>distributedItemsOut</i>, it
        includes items emitted items to local processors.
    </td>
  </tr>
  
  <tr>
    <td><b>receivedCount</b></td>
    <td>
        The number of received items. This number does not include
        watermarks, snapshot barriers etc. It’s the number of items the
        Processor.process method will receive.
    </td>
  </tr>
  
  <tr>
    <td><b>receivedBatches</b></td>
    <td>
        The number of received batches. <code>Processor.process</code>
        receives a batch of items at a time, this is the number of such
        batches. By dividing <i>receivedCount</i> by
        <i>receivedBatches</i>, you get the average batch size. It will
        be 1 under low load.
    </td>
  </tr>

  <tr><td colspan="3" class="divider"></td></tr>

  <tr>
    <td><b>numInFlightOps</b></td>
    <td>
        The number of pending (in flight) operations when using
        asynchronous mapping processors. See
        <a href="/javadoc/{jet-version}/com/hazelcast/jet/core/processor/Processors.html#mapUsingServiceAsyncP-com.hazelcast.jet.pipeline.ServiceFactory-int-boolean-com.hazelcast.function.FunctionEx-com.hazelcast.function.BiFunctionEx-">
        Processors.mapUsingServiceAsyncP</a>.
    </td>
    <td rowspan="9">
        <b>job, exec, vertex, proc, procType</b><br>
        <br>
        Processor specific metrics, only certain types of processors
        have them. The <i>procType</i> tag can be used to identify the
        exact type of processor sourcing them. Like all processor
        metrics, each Jet member will have one instances of these
        metrics for each processor instance N, the N denotes the global
        processor index.
    </td>
  </tr>

  <tr>
    <td><b>totalKeys</b></td>
    <td>
        The number of active keys being tracked by a session window
        processor.
    </td>
  </tr>

  <tr>
    <td><b>totalWindows</b></td>
    <td>
        The number of active windows being tracked by a session window
        processor. See
        <a href="/javadoc/{jet-version}/com/hazelcast/jet/core/processor/Processors.html#aggregateToSessionWindowP-long-long-java.util.List-java.util.List-com.hazelcast.jet.aggregate.AggregateOperation-com.hazelcast.jet.core.function.KeyedWindowResultFunction-">
        Processors.aggregateToSessionWindowP</a>.
    </td>
  </tr>

  <tr>
    <td><b>totalFrames</b></td>
    <td>
        The number of active frames being tracked by a sliding window
        processor.
    </td>
  </tr>

  <tr>
    <td><b>totalKeysInFrames</b></td>
    <td>
        The number of grouping keys associated with the current active
        frames of a sliding window processor. See
        <a href="/javadoc/{jet-version}/com/hazelcast/jet/core/processor/Processors.html#aggregateToSlidingWindowP-java.util.List-java.util.List-com.hazelcast.jet.core.TimestampKind-com.hazelcast.jet.core.SlidingWindowPolicy-long-com.hazelcast.jet.aggregate.AggregateOperation-com.hazelcast.jet.core.function.KeyedWindowResultFunction-">
        Processors.aggregateToSlidingWindowP</a>.
    </td>
  </tr>

  <tr>
    <td><b>lateEventsDropped</b></td>
    <td>
        The number of late events dropped by various processor, due to
        the watermark already having passed their windows.
    </td>
  </tr>
</table>

### User-defined Metrics

User-defined metrics are actually a subset of
[job metrics](#job-specific-metrics). What distinguishes them from
regular job-specific metrics is exactly what their name implies: they
are not built-in, but defined when processing pipelines are written.

Since user-defined metrics are also job metrics, they will have all the
tags job metrics have. They also have an extra tag, called `user` which
is of type `boolean` and is set to `true`.

> Due to the extra tag user-defined metrics have it’s not possible for
> them to overwrite a built-in metric, even if they have the exact
> same name.

Let’s see how one would go about defining such metrics. For example if
you would like to monitor your filtering step you could write code like
this:

```java
p.readFrom(source)
 .filter(l -> {
     boolean pass = l % 2 == 0;
     if (!pass) {
         Metrics.metric("dropped").increment();
     }
     Metrics.metric("total").increment();
     return pass;
 })
 .writeTo(sink);
```

For further details consult the [Javadoc](/javadoc/{jet-version}/com/hazelcast/jet/core/metrics/Metrics.html).

User-defined metrics can be used anywhere in pipeline definitions where
custom code can be added. This means (just to name the most important
ones): filtering, mapping and flat-mapping functions, various
constituent functions of aggregations (accumulate, create, combine,
deduct, export & finish), key extraction function when grouping, in
[custom batch sources](../how-tos/custom-batch-source.md),
[custom stream sources](../how-tos/custom-stream-source.md),
[custom sinks](../how-tos/custom-sink.md), processors and so on.

### Exposing Metrics

The main method Jet has for exposing the metrics to the outside world is
the JVM’s standard **JMX** interface. Since Jet 3.2 there is also an
alternative to JMX for monitoring metrics, via the Job API, albeit
only the job-specific ones.

#### Over JMX

Jet exposes all of its metrics using the JVM’s standard JMX interface.
You can use tools such as **Java Mission Control** or **JConsole** to
display them. All Jet-related beans are stored under
`com.hazelcast.jet/Metrics/<instanceName>/` node and the various tags
they have form further sub-nodes in the resulting tree structure.

Hazelcast metrics are stored under the
`com.hazelcast/Metrics/<instanceName>/` node.

#### Prometheus

Prometheus is a popular monitoring system and time series database.
Setting up monitoring via Prometheus consists of two steps. First step
is exposing an HTTP endpoint with metrics. The second step is setting up
Prometheus server, which pulls the metrics in a specified interval.

The Prometheus javaagent is already part of the Hazelcast Jet
distribution and just needs to be enabled. Enable the agent and expose
all metrics via HTTP endpoint by setting an environment variable
PROMETHEUS_PORT, you can change the port to any available port:

```bash
PROMETHEUS_PORT=8080 bin/jet-start
```

You should see following line printed to the logs:

```text
Prometheus enabled on port 8080
```

The metrics are available on [http://localhost:8080](http://localhost:8080).

For a guide on how to set up Prometheus server go to the
[Prometheus website](https://prometheus.io/docs/prometheus/latest/getting_started).

#### Via Job API

The `Job` class has a
[`getMetrics()`](/javadoc/{jet-version}/com/hazelcast/jet/Job.html#getMetrics--)
method which returns a
[JobMetrics](/javadoc/{jet-version}/com/hazelcast/jet/core/metrics/JobMetrics.html)
instance. It contains the latest known metric values for the job.

This functionality has been developed primarily to give access to
metrics of finished jobs, but can in fact be used for jobs in any state.

For details on how to use and filter the metric values consult the
[JobMetrics API docs](/javadoc/{jet-version}/com/hazelcast/jet/core/metrics/JobMetrics.html)
. A simple example for computing the number of data items emitted by a
certain vertex (let’s call it vertexA), excluding items emitted to the
snapshot, would look like this:

```java
Predicate<Measurement> vertexOfInterest =
        MeasurementPredicates.tagValueEquals(MetricTags.VERTEX, "vertexA");
Predicate<Measurement> notSnapshotEdge =
        MeasurementPredicates.tagValueEquals(MetricTags.ORDINAL, "snapshot").negate();

Collection<Measurement> measurements = jobMetrics
        .filter(vertexOfInterest.and(notSnapshotEdge))
        .get(MetricNames.EMITTED_COUNT);

long totalCount = measurements.stream().mapToLong(Measurement::value).sum();
```

### Configuration

The metrics collection is enabled by default. You can configure it using
the hazelcast.yml file (these are not strictly Jet, but Hazelcast
node properties). See also [full guide](../operations/configuration.md)
on configuration.

```yaml
hazelcast:

  metrics:
    # whether metrics collection should be enabled
    enabled: true
  
    # metrics collection interval in seconds
    collection-frequency-seconds: 5
  
    jmx:
      # whether metrics should be exposed over JMX
      enabled: true
```
