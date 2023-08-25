---
title: 021 - Simple Rolling Upgrade for SQL
description: Describe the simple rolling upgrade implementation by using members with the same version 
---

Until now, Jet didn't support rolling upgrades at all. Since Jet is
being used to back SQL, SQL will be completely unavailable while there
are members of different versions in the cluster.

Proper implementation of rolling upgrade would require:

- binary compatibility for all serialized classes: processor suppliers,
  all lambdas and the fields they capture, all jet operations, stream
  items, snapshot objects, job metadata in IMaps

- compatible behavior of processors, operations, client messages and
  of packets sent directly between members

- SQL optimizer will have to be able to avoid using of new processor
  implementations or behavior until the cluster is upgraded

While the above is theoretically possible, it will add a large burden to
development of every new feature and a whole new class of possible
compatibility issues. For this reason we decided to do a simplified
approach: a job will run only on a subset of members that have the same
version. We'll use the largest such subset. We'll also ensure that a
member of the same version will create the execution plan for the query.
This allows us to ignore most of the above compatibility requirements.

When comparing versions, we'll ignore the patch version - we'll provide
full rolling upgrade support for patch versions.

In the traditional upgrade procedure when members are upgraded one by
one, in the middle of the upgrade process only half of the members will
actually run Jet jobs - when half of the members are of version `v` and
half are of version `v+1`. If the two groups of members with the same
version have the same size, we choose the group with higher version; the
reason is that members of `v+1` version will not be shut down. To
mitigate this, the user can choose to temporarily increase the cluster
size, the enterprise licence allows it.

## Non-goals

This feature won't provide rolling upgrades for Jet light jobs in
general. To do that we would have to provide Jet client compatibility
and DAG binary compatibility.

Fault-tolerant SQL jobs also won't be supported.

## Implementation details

### Only implemented for light jobs

We implemented this feature only for light jobs. Normal jobs can be
fault-tolerant - to support restarting of a job on a new version, we
would need most of the compatibility requirements listed above.

### SQL operations routing

SQL commands submitted from a member are optimized and submitted
locally, but only to members with the same version as the coordinator.
Even if it's the smaller same-version group.

If the operation is sent from a client, it's routed to a member from the
larger group. For a non-smart client, the receiving member can redirect
the operation.

### Race in member upgrades

It can happen that the coordinator sends `InitExecutionOperation` to
some member, but the member changed version in the meantime. This can
happen because operations are routed using `Address` and the address can
be reused by the upgraded member. To resolve this, we added
`coordinatorVersion` parameter to `InitExecutionOperation`. The target
will check it and throw if its version is not equal to the received
version.

### Shutdown changes

During rolling upgrades, members are gracefully shut down. We want SQL
engine to be available during this time. We'll add a new
`JobConfig.blockShutdown` option. The SQL engine will enable this option
for batch jobs, but not for streaming jobs.

Jobs submitted during this time will not use the member in the shutdown
process as a coordinator. Other members can handle new jobs, but they
will not use the members being shut down. To do this, the existing
`NotifyMemberShutdownOperation` will be broadcast to all members
(currently it's sent just to the master).

The shutdown procedure will block until all jobs with this option
complete.

Fault-tolerant jobs will be shut down immediately with a snapshot,
regardless of this option. Other jobs (non-fault-tolerant ones without
this option enabled) will be cancelled (light jobs) or restarted (normal
jobs).

It can happen that the member will never shut down if the user submits a
streaming job with the `blockShutdown` option. For this scenario there
is a hard-coded timeout of 10 seconds after which the member proceeds
with the shutdown without waiting for all jobs to terminate. In this
case the administrator must be able to see that job and cancel it
manually, using either Management center or Java API. Another option is
to kill the member, however in this case a proper migration will not
take place.

### Client compatibility

A client able to communicate with multiple versions of members is a
prerequisite for rolling upgrades. When using Jet as a backing engine
for SQL, the jobs are never submitted from the client. Instead, the
client submits a SQL string using the SQL client messages, and on the
member it is converted to a DAG and submitted to Jet. We must make sure
that the member that optimizes the query is also the member that
coordinates the Jet job. Otherwise, the processor behavior might not be
compatible.

We don't plan to provide client compatibility for JetService in 5.0.

### Understandable error messages for unsupported scenarios.

Unsupported features must fail with a clear error message. E.g. a normal
job must fail with appropriate error message after a member with a newer
version is added, even in a fault-tolerant job.

### Changes to the SQL engine

For non-smart client, the `SqlExecute` client operation will route to a
random member - it might not be from the larger same-version group. To
address this, we need to add member-to-member variants of `SqlExecute`,
`SqlFetch` and `SqlCancel` operations so that a query submitted from a
non-smart client can be redirected to execute on a member from the
larger same-version subset. This will also address the current limitation
that SQL queries can't be submitted from lite members.

### LoadBalancer changes

`com.hazelcast.client.LoadBalancer` is public API. Has 3
implementations: `StaticLB`, `RoundRobinLB` and `RandomLB`. The
`AbstractLoadBalancer` base implementation is also public API.

In 4.2 `LoadBalancer.nextDataMember()` method was added that throws
`UnsupportedOperationException` in the default implementation. In 4.2,
the SQL API required that the query was submitted to a data member. If
the user had his own LB implementation, SQL wouldn't work from clients
at all unless the LB was modified too.

Due to rolling upgrades, we need another strategy to choose the target
member: any data member from the larger same-version group. It's not
possible to add this without requiring a non-trivial implementation from
the users in their LB implementations. Therefore we decided to not use
the `LoadBalancer` interface for choosing the member for executing SQL.
Instead, we'll manually choose a random member from the desired group.

We'll deprecate the `LB.nextDataMember()` method and mark it as unused.

`LoadBalancer` is used only for smart clients. Non-smart clients connect
directly to just one member which needs to forward the request if it's
not from the SQL group.

### Handling of client connections

Queries submitted from a client should be cancelled when that client
disconnects. Before, the client operation was always handled by the
coordinating member, but now it could be forwarded to a different
member. To handle this, we need to add two actions:

1. the member that received the client message, will monitor the client,
and if it disconnects, it will send the `SqlCloseOperation` to the
coordinator to cancel the job.
   
2. the job coordinator will cancel the job if the submitting member
leaves

### Client security

If security in Hazelcast Enterprise is enabled, for queries submitted
from a client we must check permissions. However, the member handling
the client request isn't able to check them until it parses and
validates the query. And since the query can be forwarded to a different
member, we need to forward the authentication information to the
coordinating member. Therefore we add `Collection<Principal>` to the
`SqlExecuteOperation`. If this collection is not null, the recipient
will check if some of the principals has access to the objects
referenced in the query. If this collection is empty, the query will be
rejected (no access to anything).

### Partition assignment

Before, the partition assignment for the job always mirrored the
assignment for the cluster. That means, the `partitioned` edges sent
objects with a particular partitioning key to the same member where that
partitioning key would be located in an IMap. Therefore, processors
accessing HZ data structures were assigned partitions that were local.

However, this wasn't a strict requirement and only was true initially.
After a partition migration, the job was able to continue to run - just
the operations to HZ data structure became remote.

For rolling upgrades, if the job runs on a subset of members, we can't
match the partition table for the job even initially. This means
somewhat reduced performance because part of the operations will be
remote, but the correctness of results will be preserved.

## Implementation parts

The implementation will be split into 3 parts:

**PR#1**: Coordinator using only same-version members, smart clients
sending to a correct member.

**PR#2**: The [Shutdown changes](#shutdown-changes)

**PR#3**: Forwarding of the client operations

The PR#3 will likely not be implemented for 5.0 and is not strictly
required. It will affect only non-smart client and only in a way that
they will use less members than they could.

## Parts that will need to support backwards compatibility

- The client protocol

- The `SqlExecute`, `SqlFetch` and `SqlClose` member-to-member
operations.

- The IMap scan operations `MapFetchEntriesOperation`,
`MapFetchIndexOperation`
