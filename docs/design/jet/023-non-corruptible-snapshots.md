---
title: 023 - Non-corruptible snapshots
description: Job snapshots should survive single node crash in any case
---

## Summary

Jet uses snapshots to restore job state after various events including node failure and cluster topology changes.
However, snapshot data is written to `IMap` which is AP data structure, prone to data loss in 
[specific circumstances](https://docs.hazelcast.com/hazelcast/5.2/architecture/data-partitioning#best-effort-consistency).
If this happens during snapshot, snapshot may [become corrupted](https://github.com/hazelcast/hazelcast/issues/18675).

Snapshot should be safe in case of single node misbehavior or failure.

## Terminology

- **automatic snapshot** - snapshot collected periodically to provide fault tolerance and processing guarantees
- **named snapshot** - snapshot exported manually on user request 
- **terminal snapshot** - snapshot after which the job will be cancelled

## Current snapshotting algorithm

Jet uses [2-phase snapshot algorithm](https://hazelcast.com/blog/transactional-connectors-in-hazelcast-jet/)
which from the point of view of snapshot data consistency can be summarized as follows.

### Snapshot taking procedure

1. Initiate snapshot: generate new `ongoingSnapshotId`, notify all processors
2. 1st snapshot phase (`saveSnapshot` + `snapshotCommitPrepare`):
   Each processor instance writes its state to `IMap` as "chunk"
3. Make decision: if all processors succeeded in 1st phase then the snapshot will be committed in 2nd phase,
   otherwise it will be rolled back.
4. Save decision to `IMap` as `SnapshotVerificationRecord` and in `JobExecutionRecord`. 
   `JobExecutionRecord` contains also updated `snapshotId`. 
5. Delete previous snapshot data.
6. 2nd snapshot phase (`snapshotCommitFinish`) with decision made earlier. 
   This step can be performed concurrently with processing of next items and must ultimately succeed.
7. Schedule next snapshot

Snapshotting uses alternating pair of maps `__jet.snapshot.<jobId>.<0 or 1>`:
one contains the last successful snapshot, the other one contains the snapshot currently being performed (if any).
They are selected using ongoing data map index.
These maps also contain `SnapshotVerificationRecord`.

Each "chunk" is tagged with `snapshotId` to which it belongs.

Exported snapshots (with exception for terminal ones) are handled specially:
- they only save state
- they do not rollback or commit any transactions when they are taken (2nd phase is mostly a no-op) 
- they are registered in `exportedSnapshotsCache` so they can be listed, 
  but can be used to start job even if they are not present on the list
  (this applies also for terminal exported snapshots)

### Snapshot restore procedure

1. Read current snapshot id from `JobExecutionRecord`.
2. Check consistency of the indicated snapshot using data in snapshot `IMap` and `SnapshotVerificationRecord`.
   If inconsistent, job fails permanently.
3. Restore job using data from snapshot. Transactions which have ids in the snapshot are committed (just in case),
   transactions that are not there are rolled back (also just in case).
   This ultimately gives consistency between job state and external transactional resources.

## Root cause of the corruption issue

During snapshot a many updates to the `IMap`s are made. Loss of any of them
due to AP properties of `IMap` causes snapshot corruption
and there is no easy way to fix such corrupted or incomplete snapshot.

## Implemented solution

### Assumptions

We decrease availability to increase consistency and provide exactly-once guarantees.
This means that the job may not be running (lower availability)
if Jet state can be inconsistent (not backed up data in `IMap`).
This may also mean that if the cluster is constantly unsafe,
the job may be never restored from the snapshot.

Amount of snapshot data shall be limited.
Storing potentially unlimited number of snapshots is not allowed.

It is allowed for the snapshot to become corrupted when the number of failed members is higher
than the sync backup count of Jet IMaps,
per standard guarantees of `IMap`. In such case exactly-once semantics is not guaranteed.

The change does not aim to protect against "zombie" operations
(operations executed very late by repetition logic).

### IMap changes

`IMap` modifying operations will throw `IndeterminateOperationStateException` 
after `hazelcast.operation.backup.timeout.millis` if not all backups have been acked in time.
Currently, in such case `IMap` operations return success.
This behavior will be configurable using private `IMap` API on IMap-proxy level.
This setting will apply only to single-key operations, it will not affect multi-key operations, for example `clear`.

Throwing `IndeterminateOperationStateException` will be enabled for:
- snapshot data `IMap`s.
- `JobExecutionRecord` map

### Updated snapshotting algorithm

New algorithm uses the fact that some `IMap` operations can end with `IndeterminateOperationStateException`
("indeterminate result" in short).

#### Snapshot taking procedure

1. Initiate snapshot: generate new `ongoingSnapshotId` (always incremented, never reused, even for exported snapshots) in `JobExecutionRecord`
2. Write `JobExecutionRecord` using safe method
   Indeterminate result or other failure -> snapshot failed to start, there is no need to rollback or commit anything.
3. Clear ongoing snapshot map.
   Indeterminate result -> not possible, however operation may not be replicated to all backups.
   Stray records are not a big problem because each record in snapshot map has `snapshotId`
   and we never reuse the same id in more than 1 attempts that could have a chance to write something to snapshot map. 
4. 1st snapshot phase (`saveSnapshot` + `snapshotCommitPrepare`): 
   Each processor instance writes its state to `IMap` as "chunks".
   Any indeterminate put -> snapshot failed
5. Write `SnapshotVerificationRecord`
   Indeterminate result -> snapshot failed
6. Make decision: if all processors succeeded in 1st phase and `SnapshotVerificationRecord` was written 
   then the snapshot will be committed in 2nd phase, otherwise it will be rolled back.
   In case of success: update in memory last good snapshotId in `JobExecutionRecord` to newly created snapshot
   and switch ongoing snapshot map index if it is not an exported snapshot.
7. Write `JobExecutionRecord` using safe method (*)
   Indeterminate result or other failure (in particular network problem, timeout) -> 
   - automatic snapshot and terminal exported snapshot:
     restart job immediately without performing 2nd phase of snapshot
     (no rollback and no commit, one of them will happen after restore).
     `MasterJobContext.requestTermination` with `RESTART_FORCEFUL` mode will be used.
     TODO: what if there is already termination in progress that waits for snapshot completion (eg. terminal snapshot export)? Maybe just allow it to continue?
   - exported snapshot: do not publish snapshot in cache, proceed to 2nd phase as if the snapshot failed.
     During restore, we can find `JobExecutionRecord` indicating that exported snapshot is still in progress.
     This information can be safely ignored, no 2nd phase for such snapshot is necessary.
8. Optionally - clear new ongoing snapshot map if the snapshot was successful to decrease memory usage.
9. 2nd snapshot phase (`snapshotCommitFinish`) with decision made earlier.
   This step can be performed concurrently with processing of next items and must ultimately succeed.
10. Schedule next snapshot

(*) We could repeat for a few times until we obtained different result than indeterminate (can be success or failure).
This does not block processing in processors but increases amount of uncommitted work that can be lost.
This is also more complicated in implementation and may be considered later if needed.

#### Snapshot restore procedure

1. Load `JobExecutionRecord` from `IMap` to `MasterContext` (skipped if the job coordinator has not changed) (*)
2. Write `JobExecutionRecord` using safe method to map from which it was read to ensure that it is replicated.
   In case of indeterminate result or other failure (in particular network problem, timeout) - do not start job now, schedule restart later.
3. Read last good snapshot id from `JobExecutionRecord`. 
   `JobExecutionRecord` contains also last snapshot id that could have written something to snapshot data `IMap`. 
4. Check consistency of the indicated snapshot using data in snapshot `IMap` and `SnapshotVerificationRecord`.
   If inconsistent, job fails permanently.
5. If restoring from exported snapshot, 
   write `SnapshotVerificationRecord` to map from which it was read to ensure that it is replicated.
   (TODO: is it necessary? exported snapshot without entry in cache can be used to restore from)
6. Restore job using data from snapshot. Transactions which have ids in the snapshot are committed (just in case),
   transactions that are not there are rolled back (also just in case).
   This ultimately gives consistency between job state and external transactional resources.

(*) Note that unless job coordinator changes, we try to proceed with the new snapshot id (saved in memory)
and write `JobExecutionRecord` for it. If we succeed, the snapshot can be committed so items do not need to be reprocessed.

#### Ensuring that JobExecutionRecord is backed up

Current method of updating `JobExecutionRecord` has the following characteristics:
1. Timestamp is updated automatically before write.
2. `JobExecutionRecord` can be updated in parallel. If some update is skipped (based on timestamp) it will not throw indeterminate state exception.
3. `MasterContext.writeJobExecutionRecord` swallows all exceptions and code invoking it does not expect exceptions.

This is not sufficient for updates during snapshot taking and restoring.
A new method `MasterContext.writeJobExecutionRecordSafe` will be introduced which:
1. Returns information if the update actually took place
2. Propagates all exceptions

`writeJobExecutionRecordSafe` will be executed in a loop until it either returns true or throws exception.
This process should not loop forever because other updates to `JobExecutionRecord` can be only caused by:
- job state updates
- quorum size updates
Both of them are not happening very frequently and constantly.

#### Correctness

TODO: make this section clearer

`JobExecutionRecord` entry version for given job and status can be in one 3 states:
1. nonexistent: there exists different version (or none only briefly when the job is created)
2. maybe safe: writing to `IMap` ended in indeterminate result, may exist on primary replica only or in primary and backups
3. safe: saved in primary and backups

Updated algorithm guarantees correctness by holding the following invariants:
1. Snapshot restore is never performed concurrently with snapshot taking
   (guaranteed by various mechanisms in Hazelcast and Jet, among others
   `MasterJobContext.scheduleRestartIfClusterIsNotSafe` and split-brain protection).
2. 2nd snapshot phase commit or rollback is executed if and only if `JobExecutionRecord` is "safe".
3. Next snapshot is performed after 2nd phase of previous snapshot (commit or rollback) has completed.
4. If `JobExecutionRecord` points to given snapshot during restore, 
   it implies that given snapshot was prepared (1st phase) successfully.
   It does not determine if 2nd phase has already completed or not. 
5. If `JobExecutionRecord` does not point to given snapshot during restore
   it means that given snapshot failed during preparation (1st phase),
   writing `SnapshotVerificationRecord` or `JobExecutionRecord`.
   There may exist some transaction that have to be rolled back.
6. If `JobExecutionRecord` was "maybe safe" and reverted to previous version
   snapshot to which it pointed when it was "maybe safe" is treated as failed snapshot cannot ever be used. 
   Reverted `JobExecutionRecord` version cannot reappear.
   In other words, change from older to newer snapshot is not possible during restore.
7. It is possible to rollback transactions prepared by snapshot that failed without data from such snapshot. 
   This is implemented using [XA protocol or workarounds](https://hazelcast.com/blog/transactional-connectors-in-hazelcast-jet/).

## Impact

### Job suspension

TBD: job suspension creates snapshot. What if it is indeterminate? 
There are already some warnings in `Job.suspend()` docs that the job can be restarted instead of suspended. 

### Job suspension on failure

Job restart due to failed snapshot (indeterminate result of `JobExecutionRecord` write)
should not trigger yet another snapshot if `suspendOnFailure` is enabled.

### Exported snapshots (Enterprise version)

Exported snapshots are performed as follows:
1. if the job is running, take snapshot but write data to `exportedSnapshot.<name>` IMap instead of ordinary snapshot data IMap
2. if the job is suspended, copy most recent automatic snapshot data to `exportedSnapshot.<name>` IMap

First case will benefit from added protection against corruption.
`IndeterminateOperationStateException` at the last stage (writing `SnapshotVerificationRecord`)
of snapshot export will fail it but it is not known if the snapshot actually succeeded (it could).
User can try to use such snapshot to restore and it might succeed or not.

Second case creates a dedicated Jet job which copies `IMap` contents using regular `readMapP` and `writeMapP`.
They will not be extended to support `failOnIndeterminateOperationState` setting,
so it will still be possible that the exported snapshot can be silently corrupted.
Before copying snapshot data it will be ensured that the automatic snapshot is valid and safe in similar way to restore,
by writing `JobExecutionRecord` and failing in case of `IndeterminateOperationStateException`.

### Rolling upgrade

Format of data in IMaps will not change.

### Performance

Most important for performance is snapshot taking as it occurs regularly.
Other processes are either manual or occur after error or topology changes
so are rare with little impact for overall performance.

In happy-path, when the cluster is stable, there is 1 additional `JobExecutionRecord` update in the `IMap`.
Other than that, nothing changes.
Jet already uses 1 sync backup for snapshot and other `IMap`s by default
and operations wait for backup ack before completing.

When the cluster is unstable, snapshot will take almost the same time
but may fail instead silently being successful with risk of corruption.

Snapshot restore has to ensure that `JobExecutionRecord` or `SnapshotVerificationRecord` is safe.
This is 1 or 2 additional `IMap` updates.

`writeJobExecutionRecordSafe` can be invoked a few times in case of concurrent `JobExecutionRecord` updates.
This increases number of additional `IMap` updates but is unlikely.