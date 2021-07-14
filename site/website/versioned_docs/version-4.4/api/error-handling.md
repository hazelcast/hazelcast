---
title: Error Handling Strategies
description: Ways of coping with unexpected error in Hazelcast Jet.
id: version-4.4-error-handling
original_id: error-handling
---

Jet may experience errors on multiple levels:

* on the *cluster level*: nodes can be removed from or added to the
  cluster at any given time, due to, for example, internal errors or
  loss of networking
* on the *integration* level: a variety of I/O errors, e.g., network or
  remote service failures
* on the *job* level:
  * *input data errors* (e.g., unexpected null values, out-of-range
    dates, malformed JSON documents)
  * *coding error* (e.g., filtering lambdas throwing
    `NullPointerException`, stateful mapping code containing memory
    leaks)

The following sections discuss options and best practices you can use on
all these levels (except the cluster level, which we cover in the
section on [fault tolerance](../architecture/fault-tolerance)).

## Integration Level

### Sources & Sinks

Sources and Sinks are Jet's points of contact with external data stores.
Their various types have specific characteristics which enable error
handling strategies only applicable to them.

For example, our [Change Data Capture
sources](sources-sinks#change-data-capture-cdc) can attempt to reconnect
automatically whenever they lose connection to the databases they
monitor, so for intermittent network failures, their owner jobs don't
need to fail.

To see the failure handling options of a given [source or
sink](sources-sinks), consult its Javadoc.

### In-Memory Data Structures

Jet comes out of the box with some [in-memory distributed data
structures](data-structures) which can be used as a data source or a
sink. These are hosted by Hazelcast clusters, which can be the same as
the one Jet runs on or separate, remote ones.

In this case, a large portion of error handling (for example,
communications failures with the remote cluster, but not just those) is
handled transparently to Jet, by the Hazelcast cluster itself. For
further details and configuration options, read the [relevant section in
the Hazelcast
manual](https://docs.hazelcast.org/docs/latest/manual/html-single/#handling-failures).

## Job Level

The primary way of dealing with errors at the job level in Jet is to
make the jobs themselves independent of each other. In a Jet cluster,
there is an arbitrary number of independent jobs running in parallel.
Jet ensures that these jobs do not interact in any way, and one's
failure does not lead to any consequences for the others.

The concern of error handling becomes: what happens to a job once it
encounters a problem. By default, Jet fails a job that has thrown an
error, but what one can do with such a failed job afterward is the
interesting part.

### Jobs Without Mutable State

For many streaming jobs, specifically the ones which don't have any
processing guarantee configured, the pipeline definition and the job
config are the only parts we can identify as state, and those are
immutable.

One option for dealing with failure in immutable-state jobs is simply
restarting them (once the cause of the failure has been addressed).
Restarted streaming jobs lacking mutable state can just resume
processing the input data flow from the current point in time.

Batch jobs don't strictly fall into this immutable-state category, but
the generic, reliable way of dealing with their error in Jet right now
is also restarting them from the beginning and having them completely
reprocess their input.

### Processing Guarantees

Streaming jobs with mutable state, those with a [processing
guarantee](../architecture/fault-tolerance#processing-guarantee-is-a-shared-concern)
set, achieve fault tolerance (cluster level error handling, in terms
of this guide) by periodically saving [recovery
snapshots](../architecture/fault-tolerance#distributed-snapshot). When
such a job fails, not only does its execution stop but also its
snapshots get deleted. This makes it impossible to resume it without
loss.

To cope with this situation, you can configure a job to be suspended in
the case of a failure, instead of failing completely. For details see
[`JobConfig.setSuspendOnFailure`](/javadoc/4.4/com/hazelcast/jet/config/JobConfig.html#setSuspendOnFailure(boolean))
and
[`Job.getSuspensionCause`](/javadoc/4.4/com/hazelcast/jet/Job.html#getSuspensionCause()).

Note: this option was introduced in Hazelcast Jet 4.3. In order to
preserve the behavior from older versions, it is not the default, so it
must be enabled explicitly.

A job in the suspended state has preserved its snapshot, and you can
resume it without loss once you have addressed the root cause of the
failure.

In the open-source version of Jet this scenario is limited to fixing the
input data by some external means and then simply [resuming the
job](../operations/job-management#restarting) via the client API.

The Enterprise version of Jet  has the added option of [job
upgrades](../enterprise/job-update). In that case you can:

* export the latest snapshot
* update the pipeline, if needed, for example, to cope with unexpected
  data
* resubmit a new job based on the exported snapshot and the updated
  pipeline

One caveat of the suspend-on-failure feature is that the latest snapshot
is not a "failure snapshot." Jet can't take a full snapshot right at the
moment of the failure, because its processors can produce accurate
snapshots only when in a healthy state. Instead, Jet simply keeps the
latest periodic snapshot it created. Even so, the recovery procedure
preserves the at-least-once guarantee.
