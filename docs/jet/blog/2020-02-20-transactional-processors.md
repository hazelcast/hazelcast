---
title: Transactional connectors in Hazelcast Jet
author: Viliam Ďurina
authorImageURL: https://en.gravatar.com/userimage/154381144/a68feb9e86a976869d646e7cf7669510.jpg
---

![Transaction Processors Featured
Image](assets/2020-02-20-transactional-processors-featured-img.png)

Hazelcast Jet is a distributed stream processing engine which supports
exactly-once semantics even in the presence of cluster member failures.
This is achieved by snapshotting the internal state of the processors at
regular intervals into a reliable storage and then, in case of a
failure, using the latest snapshot to restore the state and continue.

However, the exactly-once guarantee didn't work with most of the
connectors. Only [replayable
sources](/docs/architecture/fault-tolerance),
such as Apache Kafka or IMap Journal were supported. And no sink
supported this level of guarantee. Why was that?

The original snapshot API had only one phase. A processor was asked to
save its state at regular intervals and that was it. But a sink writes
items to some external resource and must commit if the snapshot was
successful; and it must not commit if it wasn't. It also needs to ensure
that if some processor committed, all will commit, even in the presence
of failures. This is where distributed transactions come to the rescue.

## Distributed transactions

Jet uses the two-phase commit algorithm to coordinate individual
transactions. The basic algorithm is simple:

1. The coordinator asks all participants to prepare for commit

2. If all participants were successful, the coordinator asks them to
   commit. Otherwise it asks all of them to roll back

For correct functionality it is required that if a participant reported
success in the first phase, it must be able to commit when requested.

Jet acts as a transaction coordinator. Individual processors (that is
the parallel workers doing the writes) are adapters to actual
transactional resources, that is to databases, message queues etc. So
even if you have just one transactional connector in your pipeline, you
have multiple participants of a distributed transaction, one on each
cluster member.

## Two-phase snapshot procedure

The commit procedure in Jet is tied to the life cycle of the snapshot.
When a snapshot is taken, the previous transaction is committed and a
new one is started. The snapshot also serves as the durable storage for
the coordinator.

Since Jet 4.0, the snapshot has two phases. In the first phase the
participants prepare, in the second phase they commit. Important thing
is that the snapshot is successful and can be used to restore the state
of a job after the 1st phase is successful. If the job fails before
executing the 2nd phase, that is without executing the commits, the
processors must be able to commit the transactions after the job
restart. To do so, they store transaction IDs to the snapshot. This is
the basic process:

1. When a processor starts, it opens transaction `T0`. It writes
   incoming items, but doesn't commit.

2. Later the processor is asked to do the 1st phase of the snapshot (the
   `snapshotCommitPrepare()` method). The processor prepares `T0`,
   stores its ID to the snapshot and starts `T1`.

3. Items that arrive until the 2nd phase occurs are handled using `T1`.

4. When a coordinator member receives responses from all processors that
   they successfully did 1st phase, it marks the snapshot as successful
   and initiates the phase-2.

5. Some time later the processor is asked to do the 2nd phase (the
   `snapshotCommitFinish()` method). The processor now commits `T0` and
   continues to use `T1` until the next snapshot.

6. The process repeats with incremented transaction ID.

Keep in mind that a failure can occur at or between any of the above
steps and exactly-once guarantee must be preserved. If it occurs before
step 2, the transaction is just rolled back by the remote system when
the client disconnects.

If it occurs between steps 2-4, items in `T1` are are rolled back by the
remote system because the transaction wasn't prepared (the XA API
requires this). But there's also `T0` that is prepared, but not
committed. After the job restarts, it will restore from a previous
snapshot (step 4 wasn't yet executed), and since `T0` isn't found in the
restored state, it will be rolled back.

If the failure occurs after step 4, then after the job restarts, it will
try to commit all transaction IDs found in the restored state. So it
will try to commit `T0`. The commit must be idempotent: if that
transaction was already committed, it should do nothing, because we
don't know if the step 5 was executed or not.

## Consistency with internal state

The 1st phase is common for transactional processors and for processors
that only save internal state. It is coordinated using the snapshot
barrier, based on the [Chandy-Lamport
algorithm](/docs/architecture/fault-tolerance#distributed-snapshot).
The consequence is that the moment at which internal processors save
their state and external processors prepare and switch their
transactions is the same. Therefore you can combine exactly-once stages
of any type in the pipeline and it will work seamlessly.

## Transactions are needed for sources too

It might seem that since sources are designed to be read, we don’t need
anything to store. But, for example, some message systems use
acknowledgements, which are in fact writes: they change the state of the
message to consumed or they delete the message.

Jet supports JMS as a source. We’ve initially implemented the JMS source
using XA transactions, but it turned out that major brokers don’t
support it or the support is buggy. For example, ActiveMQ only delivers
a handful of messages to consumers and then stops
([issue](https://issues.apache.org/jira/projects/AMQ/issues/AMQ-7369)).
Artemis sometimes loses messages
([issue](https://issues.apache.org/jira/projects/ARTEMIS/issues/ARTEMIS-2546)).
RabbitMQ doesn't support two-phase transactions at all.

Therefore for JMS source we implemented a different strategy. We
acknowledge consumption in the 2nd phase of the snapshot. But if the job
fails after the snapshot is successful but before we manage to
acknowledge, already processed messages could be redelivered, so we
store the IDs of seen messages in the snapshot and then use that to
deduplicate. If you’re interested in details, check the [source
code](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/impl/connector/StreamJmsP.java).

## Real-life issues

As mentioned above, some brokers have incorrect or buggy XA
implementation. In other cases, prepared transactions are rolled back
when the client disconnects (for example in
[MariaDB](https://jira.mariadb.org/browse/MDEV-742) or [H2
Database](https://github.com/h2database/h2database/issues/2347)) - these
systems are not usable at all. On the contrary, other implementations
keep even non-prepared transactions, such as Artemis
([issue](https://issues.apache.org/jira/browse/ARTEMIS-2559), fixed
recently). Artemis doesn't even return these transactions when calling
`recover()`, the XA API method to list prepared transactions, but those
transactions still exist and hold locks. Transaction interleaving is
mostly also not supported, this prevents us from doing any work while
waiting for the 2nd phase.

Apache Kafka, while having all the building blocks needed to implement
XA standard, has its own API. It also lacks a method to commit a
transaction after reconnection, but we’ve been able to do it by calling
a few [private
methods](https://github.com/hazelcast/hazelcast-jet/blob/master/extensions/kafka/src/main/java/com/hazelcast/jet/kafka/impl/ResumeTransactionUtil.java#L43-L64).
Also it binds transaction ID to the connection which forces us to have
multiple open connections.

## Transaction ID pool

Due to the above real-life limitations in most connectors we use two
transaction IDs interchangeably per processor. This avoids the need for
the `recover()` method to list prepared transactions, which is
unreliable or missing. Instead, we just probe known transaction IDs for
existence.

This tactic also avoids the problem with Apache Kafka that it binds the
transaction ID to a connection: we keep a pool of 2 connections in each
processor instead and we don't have to open a new connection after each
snapshot.

All connectors except for the file sink use this approach, including the
JMS and JDBC sinks
[planned](https://github.com/hazelcast/hazelcast-jet/pull/1813) for 4.1.

## Conclusion

The new feature allowed us to implement exactly-once guarantee for
sources and sinks where it previously wasn't possible. Even though these
kinds of connectors are not ideal for a distributed system because they
generally are not distributed, they still are very useful for
integration with existing systems. JMS source, Kafka sink and file sink
are available out-of-the-box in Jet 4.0.

If you consider writing your own exactly-once connector, currently you
have to implement the Core API `Processor` class. We consider
introducing some higher-level API in the future.
