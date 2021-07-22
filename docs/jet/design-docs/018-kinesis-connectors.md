---
title: 018 - Kinesis Connector
description: Source and Sink for Amazon Kinesis Data Streams
---

*Since*: 4.4

## Summary

[Amazon Kinesis Data
Streams](https://aws.amazon.com/kinesis/data-streams/) (KDS) is a
massively scalable and durable real-time data streaming service. As part
of the Amazon Web Services offering, KDS manages the infrastructure,
storage, networking, and configuration needed to stream your data at the
level of your data throughput. You do not have to worry about
provisioning, deployment, ongoing-maintenance of hardware, software, or
other services for your data streams. Also, Amazon Kinesis Data Streams
synchronously replicates data across three availability zones, providing
high availability and data durability.

The purpose of this document is to describe the implementation of
distributed Jet sources and sinks, which make it possible to read data
from and write data into Kinesis via Jet.

## Key Concepts

A **shard** is the base throughput unit of KDS. Shards help break the
stream's data flow into independent substreams, which can be processed
in parallel. Shards preserve the order of the data items they ingest
while ordering among different shards' items is undefined. One shard
provides a capacity of 1MiB/sec data input and 2MiB/sec data output. One
shard can support up to 1000 record publications per second. You will
specify the number of shards needed when you create a data stream. For
example, you can create a data stream with two shards. This data stream
has a throughput of 2MiB/sec data input and 4MiB/sec data output and
allows up to 2000 record publications per second. You can monitor
shard-level metrics in Kinesis and add or remove shards from your data
stream dynamically as your data throughput changes by resharding the
data stream.

A **record** is the unit of data stored in Kinesis. A record is composed
of a sequence number, partition key, and data blob. Data blob is the
data of interest your data producer adds to a data stream. The maximum
size of a data blob (the data payload before Base64-encoding) is 1 MiB.

A **partition key** is used to assign records to different shards of a
data stream. Items with the same partition key always belong to the same
shard. Since shards preserve the order of the items they ingest, the
ordering of records with the same partition key is also preserved. The
partition key is specified by your data producer while adding data to
KDS.

A **sequence number** is a unique identifier for each record within its
shard. Sequence numbers are assigned by KDS when a data producer
publishes data into it. They can be used as offsets of the ordered
series of records of a shard.

## APIs

Amazon offers various choices of libraries that can be used to interact
with KDS:

* **Kinesis Client Library (KCL)** and **Kinesis Producer Library
  (KPL)** are high-level libraries that are easy to use because they
  abstract away many concerns. They manage their threading policy, hide
  away the REST-based nature of Kinesis behind asynchronous constructs,
  balance load, handle failures, and react to resharding. However, all
  this convenience makes them unsuitable for building Jet connectors,
  where we need the most control possible to make choices that are
  suitable to Jet's architecture.
* **Amazon Kinesis Data Streams API** via **AWS SDK for Java** is the
  lower-level library that allows sufficient control when interacting
  with Kinesis. It consists of a simple set of [REST-based
  operations](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_Operations.html).
  Every other concern mentioned above, when discussing the high-level
  libraries, has to be handled explicitly. This is the library used in
  the Jet source and sink implementations.

## Quotas

Amazon Kinesis Data Streams enforces quite a few quotas and limits,
which our sources and sinks need to comply with:

* A single shard can ingest up to 1 MiB of data per second (including
  partition keys) or 1,000 records per second for writes.
* The maximum size of the data payload of a record is 1 MiB.
* The
  [GetRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html)
  operation can retrieve up to 10 MiB of data per call from a single
  shard and up to 10,000 records per call.
* Each shard can support up to 5 GetRecords operations per second.
* Each shard can support up to a maximum total data read rate of 2 MiB
  per second.
* The
  [ListShards](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html)
  operation has a limit of 100 transactions per second, per data stream.
  Each such transaction is able to return at most 100 shards, and if the
  stream has more, then multiple transactions need to be used for a full
  listing. (For details, see the [discovery](#discovery) section.)
* The
  [PutRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html)
  operation can write at most 500 records into the stream. Each record
  in the request can be as large as 1MiB, up to a limit of 5MiB for the
  entire request, including partition keys. Each shard can support
  writes up to 1,000 records per second, up to a maximum data write
  total of 1 MiB per second.
* The
  [DescribeStreamSummary](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStreamSummary.html)
  operation has a limit of 20 transactions per second per account. (For
  details, see the [adaptive throughput](#adaptive-throughput) section.)

## Source

The Kinesis source is a _streaming_, _distributed_, and _fault-tolerant_
data source for Jet. It supports both the _at-least-once_ and
_exactly-once_ [processing
guarantees](../architecture/fault-tolerance.md#processing-guarantee-is-a-shared-concern).

### Distribution

Being a distributed source, it has multiple instances running in each
Jet cluster member. Each instance is responsible for reading from zero,
one or more KDS [shards](#shard). Each shard will be read by exactly one
source instance, the assignment is deterministic.

Record keys, or partition keys as Kinesis calls them, are Unicode
strings, with a maximum length limit of 256 characters. The stream uses
the MD5 hash function to map these strings to 128-bit integer values.
The range of these values is thus [0 .. 2^128). Each Kinesis shard has a
continuous chunk of this range assigned to it, called the shard's hash
range. The stream assigns a record to a shard if the record's partition
key hashes into the shard's range.

In the Jet Kinesis source, we use similar logic for assigning shards to
source instances. Each of our sources gets a part of the hash range
assigned to it. We say that a source owns a specific shard if and only
if the shard's hash range's starting point is inside the source's hash
range. Any similar range matching logic would work, as long as it's
non-ambiguous.

### Discovery

Reading records from shards assigned to them is only a part of the
responsibility of sources. Sources also need a way to discover currently
active shards in the stream to take responsibility for them. Moreover,
this discovery process can't just happen once, on start-up, because
shards are dynamic, shards can be closed, and new shards can pop up at
any time. For details, see the [resharding section](#resharding).

Continuously monitoring the set of active shards in the stream is the
responsibility of **one** of the local source instances in each Jet
cluster member. This is an optimization. If all sources would run the
discovery, they would still obtain the same data, just with a multiplied
effort and cost. Monitoring means continuously polling the stream for
the list of all shards in it.

Monitoring needs to take care not to cross the rate limit imposed by
Kinesis on this operation. For details, see the [quotas
section](#quotas).

### Resharding

Kinesis supports resharding, which lets you adjust the number of shards
in your stream to adapt to changes in data flow rate through the stream.
(Amazon charges on a per-shard basis, that's why it's desirable to have
the smallest amount of shards possible.)

There are two types of resharding operations: shard **split** and shard
**merge**. In a shard split, you divide a single shard into two adjacent
shards. In a shard merge, you combine two adjacent shards into a single
shard. By "adjacent", we mean that one's hash range starts where the
other one's ends.

Splitting increases the number of shards in your stream and therefore
increases the data capacity (and cost) of the stream. Similarly, merging
reduces the number of shards in your stream and therefore decreases the
data capacity (and cost).

Resharding is always pairwise in the sense that you cannot split into
more than two shards in a single operation, and you cannot merge more
than two shards in a single operation. The shard or pair of shards that
the resharding operation acts on are called parent shards. The shard or
pair of shards that result from the resharding operation are called
child shards.

When child shards, resulting from a split or merge, activate, their
parents get deactivated and will no longer get data inserted into them.
From that point onward, data goes into the children.

### Read Order

Resharding does not suspend the stream's dataflow, while it's going on.
Data continues to be ingested into the stream, and at some point, it
just stops being put into the parent shards and starts being put into
the child shards.

The Kinesis Jet source would need to make sure that it finishes reading
from parents before reading from their children. However, this is not
possible since the children might end up being owned by an entirely
different instance of the source than their parents (for example, in a
split), possibly located in an entirely different Jet cluster member.

Moreover, it's not enough to finish reading from the parent before
reading from the children. Even if that was achieved, data from parents
might overtake data from children further down the Jet pipeline, simply
because it's a parallel flow. A Kinesis source would need to make sure
that it has read all data from the parents and that data has fully
passed through the Jet pipeline before starting to read from the
children. Only then it could provide the same ordering as KDS while
resharding.

This is currently not possible in Jet. Hopefully, future versions will
address the problem. Users of the Kinesis source need to be aware that
some data reordering might occur on resharding and try to time their
resharding activities, if possible, to utilize lulls in the data flow.

### Fault Tolerance

The Kinesis Jet source supports pipelines with both at-least-once and
exactly-once processing guarantees. It achieves this by saving KDS
offsets into its snapshots and starting the reading from saved offsets
when restarted.

The offsets are saved on a per-shard basis, and on restart, each source
instance receives all saved offsets for all shards, so it can function
properly regardless of how shards are assigned to sources after the
restart.

### Watermarks

The Kinesis source can provide native timestamps because the [record
data
structure](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_Record.html)
has a field that can be turned towards this purpose
(`ApproximateArrivalTimestamp`). However, it should be pointed out that
these watermarks are "native" only from Jet's point of view. They are
KDS ingestion times, i.e., whenever a KDS producer managed to push said
record into the data stream. We have no way of knowing what's the real
event time of a record.

Watermarks are also saved to and recovered from snapshots.

### Metrics

When receiving record batches, the [data
structure](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html#API_GetRecords_ResponseSyntax)
contains a field called `MillisBehindLatest` defined as following:

> The number of milliseconds the GetRecords response is from the
> stream's tip, indicating how far behind the current time the consumer
> is. A value of zero indicates that record processing caught up, and
> there are no new records to process at this moment.

This value can be useful for monitoring, so the sources publish it as a
per-processor metric.

### Code Example

A typical example of setting up a Kinesis source in Jet would look like
this:

```java
KinesisSources.kinesis("myStream")
  .withRegion("us-east-1")
  .withEndpoint("http://localhost:12345")
  .withCredentials("accesskey", "secretkey")
  .withRetryStrategy(RetryStrategies.indefinitely(250))
  .build();
```

The only mandatory property is the Kinesis `stream name`. The others are
optional and can be specified via a fluent builder.

If `region` is not specified, then _us-east-1_ will be used by default.

If `endpoint` is not specified, then the region's default endpoint will
be used.

If `credentials` aren't specified, then the [Default Credential Provider
Chain](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default)
will be followed.

If `retry strategy` is not specified, then a default will be used
(defined by us - retry indefinitely, with exponential backoff limited to
a maximum of 3 seconds). A source's retry strategy applies to failures
of reading records from or listing shards of a stream.

The actual source created will be of type
`StreamSource<Map.Entry<String, byte[]>>`, so basically a stream of
partition key - record data blob pairs.

## Sink

The Kinesis sink is a _distributed_, _fault-tolerant_ data sink for Jet.
It supports both _streaming_ and _batching_ pipelines. The
fault-tolerance guarantee it can offer is only _at-least-once_ since
Kinesis does not offer transaction support.

### Distribution

Being a distributed sink, it has multiple instances running in each Jet
cluster member. When used in a pipeline, this sink forces its incoming
edges to be _distributed_ and _partitioned_. The partition keys used by
the edges are the same as the Kinesis [partition keys](#partition-key).
This ensures that all data with the same partition key will end up in
the same global sink instance and the same shard.

### Flow Control

Writing data into a Kinesis Data Stream is governed by multiple
limitations:

* no more than 500 records can be written in one batch
* each record must contain no more than 1M of data
* each batch must contain no more than 5M of data
* each shard can ingest no more than 1,000 records per second

While most of these limitations are simple to enforce, the shard
ingestion rate is not. Different partition keys get assigned to a shard
based on a hashing function, so partition keys going into the same shard
can be written by different sink instances. Currently, Jet has no
capability for computing and coordinating such a per-shard rate among
all its distributed sink instances.

The sink takes a different approach to comply with this limitation. It
allows for the rate to be tripped (i.e., it doesn't attempt to prevent
it from happening), but once it gets tripped, sinks try to slow down the
amount of data they write to keep the rate violation as an occasional,
rare event and not a continuous storm.

The source achieves this flow control in two ways:

* by decreasing the send batch size; the default is the maximum of 500,
  which it will reduce, if necessary, to as low as 10 records/batch
* by adding a delay between two subsequent send actions (which can be as
  little as 100ms, a reasonable value in case of Kinesis and as much as
  10 seconds, which is a lot, but would occur only in an unreasonably
  sized stream, as far as shard count is concerned - ultimately the
  owner of the stream is responsible for setting up enough shards to be
  able to handle his data rates)

The flow control process is _adaptive_ in the sense that:

* it kicks in only when batches start failing due to shard ingestion
  rates being tripped
* as long as failures repeat, it keeps quickly increasing the sleep
  delays to stop them from happening
* once failures stop, it slowly decreases the sleep delays until they
  are eliminated (i.e., the data volume spike was only temporary) or
  until failures start happening again

Under normal circumstances, if there are enough shards in the stream and
their data ingestion rate covers the data flow, this whole flow control
process stays shut off. The sink publishes data with the lowest possible
latency.

### Discovery

As we've seen in the [flow control](#flow-control) section, one element
used to control the throughput is batch size. Under normal conditions,
the sink uses the default/maximum batch size of 500. When flow control
kicks in, a new batch size is picked as a function of the number of open
shards in the stream.

For this to happen, the sinks need to have a relatively up-to-date
information about the number of open shards. The sink achieves this by
using a mechanism very similar to the [discovery process employed by the
source](#discovery). The only real difference is that the sinks use the
[DescribeStreamSummary](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStreamSummary.html)
operation instead of the
[ListShards](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html)
one.

### Write Order

Under normal circumstances, the Kinesis sink preserves the order of
items belonging to the same partition key. However, when the [flow
control](#flow-control) mechanism kicks in, the ordering might be lost
on occasion.

This fact originates in the way how KDS handles shard ingestion rate
violations. When KDS receives a batch to be ingested, it processes each
item in it one by one, and if some fail, it doesn't stop processing the
batch. The result is that some items from a batch get rejected, some get
ingested, but in a random manner. The sink does resend the non-ingested
item, they won't get lost, but there is nothing it can do to preserve
the initial ordering.

The advice we can give to Kinesis sink users, if they care about
ordering at all, is to try to have enough shards to accommodate even
occasional spikes in their data rate and to make sure that their
partition keys are spread out adequately over all shards.

### Fault Tolerance

Since there is no transaction support in Kinesis, the sink can't support
exactly-once delivery. It can, however, support at-least-once
processing. It does that by ensuring it flushes all data it has taken
ownership of (taken from the `Inbox` is the more accurate "dev-speak")
out to Kinesis, before saving its snapshots.

A further reason why exactly-once support is not possible is the API
used to implement the sink, the AWS SDK itself. It has internal retry
mechanisms, which can lead to duplicate publishing of records. For
details, see the [relevant parts of its
documentation](https://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-duplicates.html#kinesis-record-processor-duplicates-producer).

### Metrics

Two metrics that should be useful to populate on a per-sink basis are
parameters related to [flow control](#flow-control):

* batch size
* sleep between two consecutive send attempts

### Code Example

A typical example of setting up a Kinesis sink in Jet would look like
this:

```java
KinesisSinks.kinesis("myStream")
  .withRegion("us-east-1")
  .withEndpoint("http://localhost:12345")
  .withCredentials("accesskey", "secretkey")
  .withRetryStrategy(RetryStrategies.indefinitely(250))
  .build();
```

The properties here work exactly like the ones for the
[source](#code-example). What's worth noting, though, is that this
version is a simplified form. It is able to accept only input items of
the form of `Map.Entry<String, byte[]>` (so partition key - data blob
pairs).

A more generic form, which can accept any item stream, is of the form:

```java
KinesisSinks.kinesis(
  @Nonnull String stream,
  @Nonnull FunctionEx<T, String> keyFn,
  @Nonnull FunctionEx<T, byte[]> valueFn
)
```

It has two more mandatory parameters:

* a `key function` that specifies how to compute the partition key from
  an input item
* a `value function` that specifies how to compute the data blob from an
  input item

## Testing

Both the Kinesis source and sink can be covered by integration tests in
which the AWS backend is mocked with the help of
[LocalStack](https://github.com/localstack/localstack) and
[Testcontainers](https://www.testcontainers.org/).

This mock is pretty reliable, with only small disadvantages. One of them
is that it doesn't enforce the intake rate of shards, so we can't write
tests to verify the sink's flow control behavior when trying to publish
more data than the stream can ingest. Another disadvantage is that it
ignores credentials (accepts anything), so we can't test behavior when
credentials are incorrect. These scenarios can, however, be tested
manually on the real AWS backend.

## Future Improvements

One extra Kinesis connector we could add to Jet in the future would be a
version of the source which supports _enhanced fan-out_. Such a Kinesis
consumer is different in two ways: it has dedicated throughput, and it
gets data pushed to it, doesn't have to poll. Implementing such a source
in future versions, though, needs to be motivated with concrete needs.

Another future improvement would be adding a generic mechanism to Jet,
which would enable us to solve the [ordering problem when
resharding](#read-order). This would be some kind of signaling mechanism
we could use in a Kinesis source to check that certain previously
dispatched items have cleared the entire pipeline. It's not clear how
exactly this would work and if it will be implemented at all.
