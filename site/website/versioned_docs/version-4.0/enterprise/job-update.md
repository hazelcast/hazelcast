---
title: Updating Jobs
description: Update Streaming Jobs without losing computation state
id: version-4.0-job-update
original_id: job-update
---

A streaming job can be running for days or months without disruptions
and occasionally it may be necessary to update the job's pipeline while
at the same preserving its state.

For example, imagine a streaming job which does a windowed aggregation
over a 24-hour window, and due to new business requirements, an
additional need is that an additional 1-hour window must be added to
the pipeline and some additional filter needs to be added.

While implementing this requirement, we don't want to lose the current
computational state because it includes data for the past day and in
many cases we may not be able to replay this data fully.

## Updating The Job

### Updating The Pipeline

The first step you must when updating a job is to update its pipeline.
The new pipeline must be _state-compatible_ with the old one, meaning
that new pipeline must be able to work with the previous state.

The following general points must be followed:

* You can add new [stateful stages](../api/stateful-transforms) and
  remove existing ones without breaking compatibility. This includes
  adding/removing a source or sink or adding a new aggregation path from
  existing sources.
* You can freely add, remove or change [stateless stages](../api/stateless-transforms),
  such as `filter`/`map`/`flatMap` stages, non-transactional sinks among
  others.
* The stage names must be preserved between job versions - naming stages
  explicitly is suggested.

For additional details, please see the [state compatibility guide](#state-compatibility).

### Exporting A Snapshot

To update a job, the first thing we need to do is to take a snapshot of
its current state and cancel it. This can be achieved by using the
following command:

```bash
bin/jet save-snapshot -C <job name or id> <snapshot name>
```

This will take an in-memory snapshot of the job state and cancel it. The
job will not process any data after the snapshot was taken and is
cleanly taken down. You can see a list of current exported snapshots in
the cluster with the following command:

```bash
$ bin/jet list-snapshots
TIME                    SIZE (bytes)    JOB NAME                 SNAPSHOT NAME
2020-03-15T14:37:01.011 1,196           hello-world              snapshot-v1
```

### Starting the Updated Job

When submitting a job, it's possible to specify an initial snapshot to
use. The job will then start with processing state restored from the
specified snapshot and as long as _state compatibility_ is maintained,
it will continue running once the snapshot is restored. To submit a job
starting from a specific snapshot you can use the following command:

```bash
bin/jet submit -s <snapshot name> <jar name>
```

## Memory Requirements

Internally, Jet stores these snapshots in `IMap`s that are separate from
the periodic snapshots that are taken as part of the job execution.
Exporting snapshots requires enough available memory in the cluster to
store the computation state.

## State Compatibility

The state has to be compatible with the updated pipeline. As a Jet
pipeline is converted to a [DAG](../architecture/distributed-computing),
the snapshot contains separate data for each vertex, identified by the
transform name. The stateful transforms in the previous and the updated
pipeline must have the same name for the state to be restored
successfully. Once the job is started again from a snapshot, the
following rules are applied:

* If a transform was not in the previous version and is available in the
  new version, the transform will be restored with an empty state.
* If a transform was in the previous version and is not in the new
  version, then its state will simply be ignored.
* If the transform existed in the previous version and also exists in
  the new version and their names match, then the state from the
  previous version will be restored as the state of the new transform.

Using these rules the following is possible:

* you can add new stateful stages and remove existing ones without
  breaking compatibility. This includes adding/removing a source or sink
  or adding a new aggregation path from existing sources.
* you can freely add, remove or change _stateless_ stages, such as
  filter/map/flatMap stages, transactional sinks and others

You can find information about what state is stored under the Javadoc
for each transform. Here are some examples of other supported changes:

* adding new sources/sinks to existing pipeline stages
* adding new branches to existing pipeline stages
* change session window timeout
* change connection parameters of sources/sinks
* enable/disable early results for a window
* for sliding windows or tumbling windows you can increase or reduce the
  window size or the slide length
* change eviction timeout for stateful map
* change parameters of aggregate operation: for example, change the
  comparator of `AggregateOperation.minBy()`
* tweak the aggregate operation, but accumulator type has to stay the
  same
* any change to stateless stages, for example updating a service in
  `mapUsingService` or updating the python module in `mapUsingPython`
* renaming a stateless stage

The following changes are not supported:

* change a sliding window to a session window
* replace aggregation operation for another one with a different
  accumulator
* rename a stateful stage

### Caveats when changing window parameters

Changing window parameters is supported, but there can be a few quirks
during the transition period. Jet accumulates events into windows as
they arrive. When you change window size, slide step or session window
timeout, it can not split already aggregated events into different
accumulators, however it will assign existing pre-aggregated values into
frames based on their end time. After all the data based on events that
were processed before the update are emitted, the results will be
correct again.

Second issue is if you extend the window size. Since Jet purges data
when the windows for which they were needed are fully emitted, if you
extend the window length, after the update Jet will need data that were
already purged. As a result, after the update the windows can miss some
data. Again, after the transition period elapses, all output will be
correct.

## Upgrading Between Jet Versions

You can also use the job update feature to update between Jet patch
versions. We eventually also plan to support updates between minor
versions, and currently this may or may not work depending on what
transforms are updated. Please note that you need to use [lossless
restart](lossless-restart) to update the Jet version without losing the
cluster state as the whole cluster needs to be restarted to update Jet
version.
