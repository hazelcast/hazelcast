---
title: Job Management
description: How to manage lifecycle of processing jobs in Jet.
---

Once a Jet job is submitted, it has its own lifecycle on the cluster
which is distinct from the submitter. Jet offers several ways to manage
the job after it's been submitted to the cluster.

## Submitting Jobs

You can submit jobs to a cluster using the `submit` command and
packaging the job as a JAR:

```bash
$ bin/jet submit -n hello-world examples/hello-world.jar arg1 arg2
Submitting JAR 'examples/hello-world.jar' with arguments [arg1, arg2]
Using job name 'hello-world'
```

For a full guide on submitting jobs, see the relevant section in the
[Programming Guide](../api/submitting-jobs).

## Listing Jobs

Use the `list-jobs` command to get a list of all jobs running in the
cluster:

```bash
$ bin/jet list-jobs
ID                  STATUS             SUBMISSION TIME         NAME
0401-9f77-b9c0-0001 RUNNING            2020-03-07T15:59:49.234 hello-world
```

You can also see completed jobs, by specifying the `-a` parameter:

```bash
$ bin/jet list-jobs -a
ID                  STATUS             SUBMISSION TIME         NAME
0402-de9d-35c0-0001 RUNNING            2020-03-08T15:14:11.439 hello-world-v2
0402-de21-7f00-0001 FAILED             2020-03-08T15:12:04.893 hello-world
```

## Cancelling Jobs

Only a batch job may complete successfully; a streaming Jet job runs
indefinitely until cancelled. You can cancel a job using the `cancel
<job_name_or_id>` command:

```bash
$ bin/jet cancel hello-world
Cancelling job id=0402-de21-7f00-0001, name=hello-world, submissionTime=2020-03-08T15:12:04.893
Job cancelled.
```

Once a job is cancelled, the snapshot for the job is lost and the job
can't be resumed. Cancelled jobs will have the "failed" status.

## Auto-scaling

By default, Jet jobs scale up and down automatically when you add or
remove a cluster node. To rescale a job, Jet must restart it. You can
find an in-depth explanation of Jet's fault tolerance design in the
[Architecture](../architecture/fault-tolerance) section.

When auto-scaling is off and you add a new node to a cluster, the job
will keep running on the previous nodes but not on the new one. However,
if the job restarts for whatever reason, Jet will automatically scale it
to the whole cluster.

The exact behavior of what happens when a node joins or leaves depends
on whether a job is configured with a processing guarantee and with
auto-scaling. The table below shows the behavior of a job after a
cluster change depending on these two settings.

|Auto-Scaling Setting|Processing Guarantee Setting|Member Added|Member Removed|
|------------|--------------------|------------|--------------|
|enabled (default)    |any setting|restart after a [delay](configuration#list-of-configuration-options)|restart immediately|
|disabled     |none|keep job running on old members|fail job|
|disabled     |at-least-once or exactly-once|keep job running on old members|suspend job|

## Suspending and Resuming

Jet supports manually suspending and resuming a streaming job in a
fault-tolerant way. The job must be configured with [a processing
guarantee](../api/submitting-jobs#setting-processing-guarantees).

When a job is suspended, all the metadata about the job is still kept in
the cluster. A snapshot of the job computational state is taken during a
suspend operation and then on a resume the job is gracefully started
from the same snapshot.

Suspending and resuming can be useful for example when you need to
perform maintenance on a data source or sink without disrupting a
running job.

Use the `suspend <job_name_or_id>` and `resume <job_name_or_id>`
commands to suspend and resume jobs:

```text
$ bin/jet suspend hello-world
Suspending job id=0401-9f77-b9c0-0001, name=hello-world, submissionTime=2020-03-07T15:59:49.234...
Job suspended.
```

```bash
$ bin/jet resume hello-world
Resuming job id=0401-9f77-b9c0-0001, name=hello-world, submissionTime=2020-03-07T15:59:49.234...
Job resumed.
```

You can also configure a job to be suspended automatically if its
execution fails (see
`[JobConfig.setSuspendOnFailure](/javadoc/{jet-version}/com/hazelcast/jet/config/JobConfig.html#setSuspendOnFailure(boolean))).

## Restarting

It's also possible to simply restart a job without suspending and
resuming in one step. This can be useful when you want to have
finer-grained control on when the job should be scaled. For example, if
you have auto-scaling off and are adding 3 nodes to a cluster you can
manually restart at the desired point to have the jobs utilizing all of
the new nodes. This can be achieved with the `restart <job_name_or_id>`
command, just as in the other examples above.
