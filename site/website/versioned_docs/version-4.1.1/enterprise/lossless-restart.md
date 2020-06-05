---
title: Lossless Cluster Restart
description: Restart a Jet cluster without losing job state
id: version-4.1.1-lossless-restart
original_id: lossless-restart
---

The Lossless Cluster Restart feature allows you to gracefully shut down
the cluster at any time and have the computational data of all the jobs
preserved. After you restart the cluster, Jet automatically restores the
data and resumes the jobs.

This allows Jet cluster to be shut down gracefully and restarted without
a data loss. When the cluster restarts, the jobs will automatically be
restarted, without having to submit them to the cluster again.

Jet jobs regularly [snapshot](../architecture/fault-tolerance) their
state. A state snapshot can be used as a recovery point. Snapshots are
stored in memory. Lossless Cluster Restart works by persisting the
RAM-based snapshot data to disk. It uses the [Hot Restart
Persistence](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#hot-restart-persistence)
feature of Hazelcast under the hood.

## Enabling Lossless Restart

To enable Lossless Cluster Restart, you need to enable it in
`hazelcast-jet.yaml`:

```yml
hazelcast-jet:
  instance:
    lossless-restart-enabled: true
```

By default the snapshot will be written to `<JET_HOME>/recovery` folder.
To change the directory where the data will be stored, configure the
`hot-restart-persistence` option in `hazelcast.yaml`:

```yml
hazelcast:
  hot-restart-persistence:
    enabled: true
    base-dir: /var/hazelcast/recovery
```

See the
[docs](https://docs.hazelcast.org/docs/4.0.1/manual/html-single/index.html#hot-restart-persistence)
for a detailed description including fine tuning.

## Safely Shutting Down a Jet Cluster

For lossless restart to work the cluster must be shut down gracefully.
When members are shutdown one by one in a rapid succession, Jet triggers
an automatic rebalancing process where backup partitions are promoted
and new backups are created for each member. This may result in
out-of-memory errors or data loss.

The entire cluster can be shut down using the `jet-cluster-admin`
command line tool.

```bash
bin/jet-cluster-admin -a <address> -c <cluster-name> -o shutdown
```

For the command line tool to work, REST should be enabled in
`hazelcast.yaml`:

```yaml
hazelcast:
  network:
    rest-api:
      enabled: true
      endpoint-groups:
        CLUSTER_READ:
          enabled: true
        CLUSTER_WRITE:
          enabled: true
        HEALTH_CHECK:
          enabled: true
        HOT_RESTART:
          enabled: true
```

Alternatively, you can shutdown the cluster using the Jet Management
Center:

![Cluster Shutdown using Management Center](assets/management-center-shutdown.png)

Since the Hot Restart data is saved locally on each member, all the
members must be present after the restart for Jet to be able to reload
the data. Beyond that, there’s no special action to take: as soon as the
cluster re-forms, it will automatically reload the persisted snapshots
and resume the jobs.

## Fault Tolerance Considerations

Lossless Cluster Restart is a _maintenance_ tool. It isn't a means of
fault tolerance.

The purpose of the Lossless Cluster Restart is to provide a maintenance
window for member operations and restart the cluster in a fast way. As a
part of the graceful shutdown process Jet makes sure that the in-memory
snapshot data were persisted safely.

The fault tolerance of Jet is entirely RAM-based. Snapshots are stored
in multiple in-memory replicas across the cluster. Given the redundancy
present in the cluster this is sufficient to maintain a running cluster
across single-node failures (or multiple-node, depending on the backup
count), but it doesn’t cover the case when the entire cluster must shut
down.

This design decision allows Jet to exclude disk from the operations that
affect job execution, leading to low and predictable latency.

## Performance considerations

Even with the fastest solid-state storage, Lossless Cluster Restart
reduces the maximum snapshotting throughput available to Jet:
while DRAM can take up to 50 GB/s, a high-end storage device caps at
2 GB/s. Keep in mind that this is throughput per member, so even on a
minimal cluster of 3 machines, this is actually 6 GB/s available to Jet.
Also, this throughput limit does not apply to the data going through the
Jet pipeline, but only to the periodic saving of the state present in
the pipeline. Typically the state scales with the number of distinct
keys seen within the time window.
