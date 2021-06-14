---
title: 021 - Simple Rolling Upgrade for Light Jobs
description: Describe the simple rolling upgrade implementation by using members with the same version 
---

Until now, Jet didn't support rolling upgrades at all. Since Jet is
being used to back SQL, SQL will be completely unavailable while there
are members of different versions.

Proper implementation of rolling upgrade would require:

- binary compatibility for all serialized classes: processor suppliers,
  all lambdas and the fields they capture, all jet operations, stream
  items, snapshot objects, job metadata in IMaps

- compatible behavior of processors, operations, client messages and
  of packets sent directly

- SQL optimizer will have to be able to avoid using of new processor
  implementations or behavior until the cluster is upgraded

While the above is theoretically possible, it will add a large burden to
development of every new feature and a whole new class of possible
compatibility issues. For this reason we decided to do a simplified
approach: a job will run only on a subset of members that have the same
version (ignoring the patch version). We'll use the largest such subset.
This allows us to ignore most of the above compatibility requirements.

In the traditional upgrade procedure, in the middle of the upgrade
process only half of the members will actually run Jet jobs - when half
of the members are of version `v` and half are of version `v+1`. If the
two groups of members with the same version have the same size, we
choose the group with higher version.

To remediate this, the user can choose to temporarily increase the
cluster size. The enterprise licence allows it.

## Implementation details

### Only implemented for light jobs

We implemented this feature only for light jobs. Normal jobs use job
metadata. They also can be fault-tolerant - to support restarting of a
job on a new version, we would need most of the compatibility
requirements listed above.

### Submit job operation routing

Light jobs submitted from a member are coordinated locally, if the local
member is a member of the larger group of members with the same version.
Otherwise, the request is sent to a random member of the larger group.

If the operation is sent from a client, it's also routed to a member
from the larger group. For a non-smart client, the receiving member
might redirect the operation.

### Race in member upgrades

When processing the `SubmitJobOperation`, the coordinator always deploys
the job to members with the same version as the coordinator, even though
the coordinator's version might no longer be a majority version.

It can also happen that the coordinator sends `InitExecutionOperation`
to a some member, but the member changed version in the meantime. This
can happen because operations are routed using `Address` and the address
can be reused by the upgraded member. To address this, we added
`coordinatorVersion` parameter to the `InitExecutionOperation`. The
target will check it and throw if its version is not equal to the
received version.

### Shutdown changes

During rolling upgrades, members are gracefully shut down. We expect SQL
engine to be available during this time. We'll add a new
`JobConfig.blockShutdown` option. The SQL engine will enable this option
for batch jobs, but not for streaming jobs.

The shutdown procedure will block until all jobs with this option
complete. Fault-tolerant jobs will be shut down immediately with a
snapshot. Other jobs (non-fault-tolerant and without this option
enabled) will block the shutdown for a configured timeout. The default
timeout will be 30 seconds.

It can happen that the member will never shut down if the user submits a
streaming job with the `blockShutdown` option. In this case the
administrator must be able to see that job and cancel it manually, using
either Management center or Java API. Another option is to kill the
member, however in this case a proper migration will not take place.

### Client compatibility

A client able to communicate with multiple versions of members is a
prerequisite for rolling upgrades. When using Jet as a backing engine
for SQL, the jobs are never submitted from the client. Instead, the
client submits a SQL string using the SQL client messages, and on the
member it is converted to a DAG and submitted to Jets. We must make sure
that the member that optimizes the query is also the member that
coordinates the Jet job. Otherwise, the processor behavior might not be
compatible.

Whether we'll provide client compatibility for JetService itself in 5.0
is an open question.

### Understandable error messages for unsupported scenarios.

Unsupported features must fail with a clear error message. E.g. a normal
job must fail with appropriate error message after a member with a newer
version is added, even a fault-tolerant job.

### Changes to the SQL engine

We need to ensure that not just the Jet jobs are run on same-version
members, but that also the sql parsing and optimization is executed on
the same version. For this reason, we'll add
`JetInstanceImpl.newLightJob()` variant that takes the version argument.
The `SqlExecute` client operation will route to a random member in the
larger same-version subset of members.

We also need to add member-to-member variants of `SqlExecute`,
`SqlFetch` and `SqlCancel` operations so that a light job submitted from
a member can be redirected to execute on a member from a larger
same-version subset. This is most important for non-smart clients
executing SQL to ensure that they're not executed by the smaller
same-version group. This wil also address the current limitation that
sql queries can't be submitted from lite members.
