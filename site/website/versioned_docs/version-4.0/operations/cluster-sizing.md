---
title: Cluster Sizing
description: Practical guide for setting up Jet clusters.
id: version-4.0-cluster-sizing
original_id: cluster-sizing
---

Jet cluster performance depends on multiple factors including the
pipeline design and user defined functions. Therefore, planning the Jet
cluster remains a complex task that requires a knowledge of the Jet
architecture and concepts. We will introduce a basic guideline that will
help you size your cluster.

We recommend always benchmark your setup before deploying it to
production. See a sample cluster sizing with benchmark that can be used
as a reference.

Please read the Hazelcast IMDG Deployment and Operations Guide when
storing the data [inside Jet cluster](architecture/in-memory-storage.md#relationship-with-hazelcast-imdg)
setup. Your Jet cluster will run both data processing and data storage
workloads so you should plan for it.

## Sizing considerations

To size the cluster for your use case, you must first be able to answer
the following questions:

* What are the throughput and latency requirements?
* How many concurrent Jobs shall the cluster run?
* Fault tolerance requirements
* How long is the error window?
* Shape of the pipelines (operations used, external systems involved)
* Characteristics of the data to be processed such as partitioning, key
  distribution and record size
* Source and sink capacity

## Determining cluster size

Even a single Jet instance running on a [recommended
server](#recommended-configuration) can host hundreds of jobs at a time.
The clustered setup improves the  performance (throughput and latency)
of hosted jobs and increases the  resilience.

A cluster with 3 members is a minimum count for fault tolerant
operations. Generally, you need ```n+1``` cluster members to tolerate
`n` member failures with the next higher odd number chosen for a split
brain detection.

Jet can utilise hundreds of CPU cores efficiently by exploiting data and
task parallelism. Adding more members to the cluster therefore helps
with scaling the CPU-bound computations. Better performance is achieved
by distributing the data partitions across the cluster to process them
in parallel.

Benchmark your jobs in a clustered setup to see the differences in
performance, see the [Sizing Example](#benchmarking-and-sizing-example).

## Sizing for failures

Jet cluster is elastic to deal with failures and performance spikes.

Elasticity is very useful feature to prevent over-provisioning. Cluster
can be up-scaled when resource consumption reaches a watermark
(autoscale isn't built in, connect Jet metrics to the resource manager)
or before expected usage spike. Up-scales however temporarily increase
the stress on the cluster as the cluster has regroup and [replay missed
data](concepts/processing-guarantees.md).

The failures reduce the cluster resources and increase the stress on
remaining members until the failed member is fixed.  The data previously
owned by the newly offline member is distributed among the remaining
members. The cluster must catch up the missed data in the stream and
keep up with the head of the stream with less CPU.

To tolerate the failure of one member, we recommend to size your cluster
to operate with ```n-1``` members setup.

You can use Hazelcast [IMap and ICache Event
Journal](https://docs.hazelcast.org/docs/jet/4.0/manual/#connector-imdg-journal)

to ingest the streaming data. Journal is an in-memory structure with a
fixed capacity. If the jobs consuming the journal can't keep up there is
a risk of data loss.  The pace of the data producers and the capacity of
the Journal therefore determine the length of an error window of your
application. If you can't afford losing data, consider increasing the
journal size or ingest streaming data using a persistent storage such as
[Apache Kafka](https://docs.hazelcast.org/docs/jet/4.0/manual/#kafka)
or Apache Pulsar.

Another approach to increase the fault-tolerance is splitting the Jet
jobs and the data storage. Streaming data can be stored in another Jet
cluster to isolate failures and performance spikes.

## Balancing cluster size with job count

The jobs running in one cluster share the resources to maximise the HW
utilisation. This is efficient for setups without a risk of [noisy
neighbours](https://searchcloudcomputing.techtarget.com/definition/noisy-neighbor-cloud-computing-performance)
 such as:

* Clusters hosting many short-living jobs
* Clusters hosting jobs with a predictable performance
* Jobs with relaxed SLAs

For stronger resource isolation (multi-tenant environments, strict
SLAs), consider starting multiple smaller clusters with resources
allocated on an OS level or using a resource manager such as
[Kubernetes](operations/kubernetes.md).

## Hardware Planning

Jet is designed to run efficiently on homogeneous clusters. All JVM
processes that participate in the cluster should have equal CPU, memory
and network resources. One slow cluster member can kill the performance
of the whole cluster.

### Minimal Configuration

Jet is a lightweight framework and is reported to run on devices such
as Raspberry Pi Zero (1GHz single-core CPU, 512MB RAM).

### Recommended Configuration

As a starting point for a data-intensive operations consider machines
such as [c5.2xlarge](https://aws.amazon.com/ec2/instance-types/c5/)
with:

* 8 CPU cores
* 16 GB RAM
* 10 Gbps network

### CPU

Jet can utilise hundreds of CPU cores efficiently by exploiting data and
task parallelism. Adding more CPU can therefore help with scaling the
CPU-bound computations. Read about the [Execution
model](architecture/execution-engine.md) to understand how Jet makes the
computation parallel and design your pipelines according to it.

By default, Jet uses all available CPU. Starting two Jet
instances on one machine therefore doesn't bring any performance benefit
as the instances would compete for the same CPU resources.

Don't rely just on CPU usage when benchmarking your cluster. Simulate
production workload and measure the throughput and latency instead. The
task manager of Jet can be configured to use the CPU aggressively as
shown in [this
benchmark](https://hazelcast.com/blog/idle-green-threads-in-jet/):
the CPU usage was close to 20% with just 1000 events/s. At 1m items/s
the CPU usage was 100% even though Jet still could push around 5m
items/s on that machine.

### Memory

Jet is a memory-centric framework and all operational data must fit to
the memory. This design leads to a predictable performance but requires
enough RAM not to run out of memory. Estimate the memory requirements
and plan with a headroom of 25% for normal memory fragmentation. For
fault-tolerant operations, we recommend reserving an extra memory to
survive the failure. See [Sizing for failures](#sizing-for-failures).

If your computation is memory-bound, consider:

* Moving data out of Jet cluster, e.g. don't use the distributed data
  structures of the Jet cluster and use the remote Hazelcast cluster
  instead.
* Scaling out, e.g. adding more members to the cluster.

Memory consumption is affected by:

* **Resources deployed with your job:** Considerable when attaching big
  files such as models for ML inference pipelines.
* **State of the running jobs:** Varies as it's affected by the shape of
  your pipeline and by the data being processed. Most of the memory is
  consumed by operations that aggregate and buffer data. Typically the
  state also scales with the number of distinct keys seen within the
  time window. Learn how the operations in the pipeline store its state.
  Operators coming with Jet provide this information in the javadoc.
* **State back-up:** For jobs configured as fault-tolerant, the state of
  the running jobs is regularly snapshotted and saved in the cluster.
  Cluster keeps two consecutive snapshots at a time (old one is kept
  until the new one is successfully created). Both current and previous
  snapshot can be saved in multiple replicas to increase data safety.
  The memory required for state back-up can be calculated as
  `(Snapshot size * 2 * Number of replicas) / Cluster member count`.
  The snapshot size is displayed in the Management Center. You might
  want to keep some state snapshots residing in the cluster as points of
  recovery, so plan the memory requirements accordingly.
* **Data stored inside Jet cluster**: Any data hosted in the Jet
  cluster. Notably the IMap and ICache Journal to store the streaming
  data. See the [Hazelcast IMDG Deployment and Operations Guide](https://hazelcast.com/resources/hazelcast-deployment-operations-guide/)
  .

### Network

Jet uses the network internally to shuffle data and to replicate the
back-ups. Network is also used to read input data from and to write
results to remote systems or to do RPC calls when enriching. In fact a
lot of Jet jobs are network-bound. Using a 10 Gbit or faster network
can improve application performance. Also consider scaling the cluster
out (adding more members to the cluster) to distribute the load.

Consider collocating Jet cluster with the data source and sink to avoid
moving data back and forth over the wire. Co-locate Jet with source
rather than a sink if you have to choose. Processed results are often
aggregated, so the size is reduced.

Jet cluster is designed to run in a single LAN. Deploying Jet cluster to
a network with high or varying latencies leads to unpredictable
performance.

### Disk

Jet is an in-memory framework. Cluster disks aren't involved in regular
operations except for logging and thus are not critical for the cluster
performance.

Consider using more performant disks when:

* You use the cluster file system as a source or sink - faster disks
 improve the performance
* Using disk persistence for [Lossless Cluster Restart](https://docs.hazelcast.org/docs/jet/4.0/manual/#configure-lossless-cluster-restart-enterprise-only)

## Data flow

Consider the capacity of data sources and sinks when planning the Jet
cluster.

Each Jet job participates in a larger data pipeline: it continuously
reads the data from the sources and writes the results to the sinks. The
capacity of all components of the data pipeline must be balanced to
avoid bottlenecks.

For slow sinks, Jet applies the back pressure and slows down the
processing and source data consumption. The data sources should be
designed to participate by reducing the pace of data production or by
buffering the data.

On the other hand, if the data source can't produce or transmit the
data fast enough, adding more resources to the Jet cluster won't bring
any performance benefits.

## Processed Data

Test your setup on a dataset that represents the characteristics of the
production data, notably:

* Partitioning of the input data
* Key distribution and count

Jet splits the data across the cluster to process it in parallel. It
builds on the prerequisite of balanced partitions to perform well.
Imbalanced partitions may create a hot spot in your cluster. The
partitioning is determined by the data source and by the grouping
keys used in the Jet application.

A frequent source of the partition imbalance are special cases: in a
payment processing application, there might be a small number of
accounts with very high  activity. Imagine a retail company account with
thousands of payments per  minute vs personal accounts with just few
payments in a day. Using account as a grouping key would lead to
imbalanced partitions. Consider special cases when designing your
pipelines and the testing data.

## Benchmarking and Sizing Example

### Requirements

The sample application is a [real-time trade
analyser](https://github.com/hazelcast/big-data-benchmark/tree/master/trade-monitor/jet-trade-monitor).
Every second, it counts the trades completed over the previous minute
for each trading symbol. Jet is also used to ingest and buffer the
stream of trades. So, the remote trading applications write trade events
to an IMap data structure in Jet cluster. The analytical job reads the
IMap Event Journal and writes processed results to a rolling file.

The job is configured to be
[fault-tolerant](concepts/processing-guarantees.md) with exactly-once
processing guarantee.

The cluster is expected to process 50k trade events per second with 10k
trade symbols (distinct keys).

### Cluster size and performance

The
[benchmark](https://hazelcast.com/resources/jet-3-0-streaming-benchmark/)
generates the expected data stream (50k events / second,  10k distinct
keys) and measures how the cluster size affects the processing latency.

We benchmarked this job on a cluster of 3, 5 and 9 nodes. We started
with a 3-member cluster as that is a minimal setup for fault-tolerant
operations.  For each topology, we benchmarked a setup with 1, 10, 20
and 40 jobs running in the cluster.

The metric we measured was latency evaluated as ```RESULT_PUBLISHED_TS -
ALL_TRADES_RECEIVED_TS``` ([learn
more](https://hazelcast.com/resources/jet-3-0-streaming-benchmark/)).
You can use this approach or design a metric that fits your application
SLAs. Moreover, our example records the maximum and average latency.
Consider measuring the result distribution, as the application SLAs are
frequently expressed using it  (e.g. app processes 99.999% of data under
200 milliseconds).

Cluster machines were of the recommended minimal configuration:
AWS [c5.2xlarge](https://aws.amazon.com/ec2/instance-types/c5/)
machines, each of 8 CPU, 16 GB RAM, 10 Gbps network.

#### 1 job in the cluster

| Cluster size | Max (ms) | Avg (ms) |
| ------------ | --- | --- |
| 3            | 182 | 150 |
| 5            | 172 | 152 |
| 9            | 215 | 134 |

#### 10 jobs in the cluster

| Cluster size | Max (ms) | Avg (ms) |
| ------------ | --- | --- |
| 3            | 986 | 877 |
| 5            | 808 | 719 |
| 9            | 735 | 557 |

#### 20 jobs in the cluster

| Cluster size | Max (ms) | Avg (ms) |
| ------------ | ---- | ---- |
| 3            | 1990 | 1784 |
| 5            | 1593 | 1470 |
| 9            | 1170 | 1046 |

#### 40 jobs in the cluster

| Cluster size | Max (ms) | Avg (ms) |
| ------------ | ---- | ---- |
| 3            | 4382 | 3948 |
| 5            | 3719 | 3207 |
| 9            | 2605 | 2085 |

### Fault-Tolerance

The [Event
Journal](https://docs.hazelcast.org/docs/4.0/manual/html-single/index.html#event-journal)
capacity was set to 1.5 million items. With a input data production rate
of 50k events each second, the data are kept for 30 seconds before being
overwritten. The job snapshot frequency was set to 1 second.

The job is restarted from the last snapshot if a cluster member fails.
In our test, the cluster restarted the processing in under 3 seconds
(failure detection, clustering changes, job restart using the last
snapshot) giving the job enough time to reprocess the 3 seconds (~ 150k
events) of data it missed.

More aggressive [failure
detector](https://docs.hazelcast.org/docs/4.0/manual/html-single/index.html#failure-detector-configuration)
and a larger event journal can be used to stretch the error window.
