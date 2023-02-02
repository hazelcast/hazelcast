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

- **automatic snapshot**, **regular snapshot** - snapshot collected periodically to provide fault tolerance and processing guarantees
- **exported snapshot**, **named snapshot** - snapshot exported manually on user request with specific IMap name 
- **terminal snapshot** - snapshot after which the job will be cancelled. Terminal snapshot may be named 
  (`cancelAndExportSnapshot`) or not (`suspend()`).
  When not named, the terminal snapshot is written in the same maps as automatic snapshot.
- **export-only snapshot** - exported snapshot that is not terminal.

## Current snapshotting algorithm

Jet uses [2-phase snapshot algorithm](https://hazelcast.com/blog/transactional-connectors-in-hazelcast-jet/)
which from the point of view of snapshot data consistency can be summarized as follows.

### Snapshot taking procedure

1. Initiate snapshot: generate new `ongoingSnapshotId`, notify all processors
2. Write `JobExecutionRecord`
3. Clear ongoing snapshot map.
4. 1st snapshot phase (`saveSnapshot` + `snapshotCommitPrepare`):
   Each processor instance writes its state to `IMap` as "chunk"
5. Make decision: if all processors succeeded in 1st phase then the snapshot will be committed in 2nd phase,
   otherwise it will be rolled back.
6. Save decision to `IMap` as `SnapshotVerificationRecord` and in `JobExecutionRecord`. 
   `JobExecutionRecord` contains also updated `snapshotId`. 
7. Delete previous snapshot data.
8. 2nd snapshot phase (`snapshotCommitFinish`) with decision made earlier. 
   This step can be performed concurrently with processing of next items and must ultimately succeed.
9. Schedule next snapshot

Snapshotting uses alternating pair of maps `__jet.snapshot.<jobId>.<0 or 1>`:
one contains the last successful snapshot, the other one contains the snapshot currently being performed (if any).
They are selected using ongoing data map index.
These maps also contain `SnapshotVerificationRecord`.

Each "chunk" is tagged with `snapshotId` to which it belongs.

Exported snapshots (with exception for terminal ones) are handled specially:
- they only save state
- they do not roll back or commit any transactions when they are taken (2nd phase is mostly a no-op) 
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

If the snapshot operation failed, the snapshot may not be safe
even though it is formally correct. User is responsible for not using such snapshot.

If the snapshot data may not be safe, the job will be restarted
instead of suspended or cancelled to decrease time for which transactions
in external data sources may be prepared but neither committed nor rolled back.
If we obeyed original intent (eg. suspend or cancel), such transaction
would be left hanging until the job is resumed from such snapshot,
which could happen much later or never.
In other words, graceful termination modes ensure that the state is "clean"
and there are no lingering transactions when the job stops executing.

If it is possible that for given snapshot at least some transactions could have been committed
or rolled back (snapshot reached 2nd phase), then all previous snapshots are outdated
and cannot be used to restart/resume job automatically.

The change does not aim to protect against "zombie" operations
(operations executed very late by repetition logic).

### IMap changes

`IMap` modifying operations will throw `IndeterminateOperationStateException` 
after `hazelcast.operation.backup.timeout.millis` if not all sync backups have been acked in time.
Currently, in such case `IMap` operations return success.
This behavior will be configurable using private `IMap` API on IMap-proxy level.
This setting will apply only to single-key operations, it will not affect multi-key operations, for example `clear`.

Throwing `IndeterminateOperationStateException` will be enabled for:
- snapshot data `IMap`s (automatic and exported)
- `JobExecutionRecord` map

### Updated snapshotting algorithm

New algorithm uses the fact that some `IMap` operations can end with `IndeterminateOperationStateException`
("indeterminate result" in short).

#### Snapshot taking procedure

1. Initiate snapshot: generate new `ongoingSnapshotId` in `JobExecutionRecord` (always incremented, never reused, even for exported snapshots)
2. Write `JobExecutionRecord` using safe method
   Indeterminate result or other failure -> snapshot failed to start, there is no need to rollback or commit anything.
   After this step new `ongoingSnapshotId` is safe and guaranteed to be unique.
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
   then the snapshot will be committed in 2nd phase (no-op for export-only snapshot), otherwise it will be rolled back.
   In case of successful, not export-only snapshot: update in memory last good snapshotId in `JobExecutionRecord` to newly created snapshot
   and switch ongoing snapshot map index if it is not an exported snapshot.
7. Write `JobExecutionRecord` depending on case:
   a) for successful automatic snapshot and terminal exported snapshot: using safe method (*)
      Indeterminate result or other failure (in particular network problem, timeout) causes 
      immediate job restart without performing 2nd phase of snapshot
      (no rollback and no commit, one of them will happen after restore).
      `MasterJobContext.handleTermination` with `RESTART_FORCEFUL` mode will be used.
      If there is another termination requests in progress that waits for snapshot completion 
      (eg. suspend() invoked when automatic snapshot was already running), it will be ignored.
   b) failed snapshot and export-only snapshot: use standard method to update `JobExecutionRecord` and ignore errors
      and performing 2nd phase of snapshot.
      During restore, we can find `JobExecutionRecord` indicating that exported snapshot is still in progress.
      This information can be safely ignored, no 2nd phase for such snapshot is necessary.
8. If 2nd phase will be performed, to decrease memory usage:
   a) Clear new ongoing snapshot map if the automatic snapshot was successful
   b) Clear snapshot map if the snapshot failed 
9. 2nd snapshot phase (`snapshotCommitFinish`) with decision made earlier.
   This step can be performed concurrently with processing of next items and must ultimately succeed.
10. Schedule next snapshot

(*) We could repeat for a few times until we obtained different result than indeterminate (can be success or failure).
This does not block processing in processors but increases amount of uncommitted work that can be lost.
This is also more complicated in implementation and may be considered later if needed.

#### Snapshot restore procedure

1. Load `JobExecutionRecord` from `IMap` to `MasterContext` (skipped if the job coordinator has not changed) (*)
2. Write `JobExecutionRecord` using safe method to ensure that it is replicated.
   This is also necessary if `JobExecutionRecord` loaded from `IMap` indicates that there was no completed snapshot yet 
   (there could one with indeterminate result).
   In case of indeterminate result or other failure (in particular network problem, timeout) - do not start job now, schedule restart.
3. Read last good snapshot id from `JobExecutionRecord.snapshotId`. 
   `JobExecutionRecord` contains also last snapshot id that could have written something to snapshot data `IMap` - `ongoingSnapshotId`. 
4. Check consistency of the indicated snapshot using data in snapshot `IMap` and `SnapshotVerificationRecord`.
   If inconsistent, job fails permanently.
5. Restore job using data from snapshot. Transactions which have ids in the snapshot are committed (just in case),
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

Job suspension creates snapshot. If the snapshot is indeterminate, job will be restarted.
Snapshot state (if the update was successful or lost) will be resolved before job execution is resumed. 

### Job suspension on failure

Job restart due to failed snapshot (indeterminate result of `JobExecutionRecord` write)
will not be treated as failure and will not suspend job if `suspendOnFailure` is enabled.
Additionally, `suspendOnFailure` does not initiate snapshot. 

### Exported snapshots (Enterprise version)

Exported snapshots are performed as follows:
1. if the job is running, take snapshot but write data to `exportedSnapshot.<name>` IMap instead of ordinary snapshot data IMap
2. if the job is suspended, copy most recent automatic snapshot data to `exportedSnapshot.<name>` IMap.

First case will benefit from added protection against corruption.
`IndeterminateOperationStateException` at the last stage (writing `SnapshotVerificationRecord`)
In addition, for terminal exported snapshot Jet will ensure that information about it
was safely written to `JobExectutionRecord`. In case of indeterminate result or crash during 2nd phase
the job will be restarted and may be restored from the just exported snapshot
(such behavior was not possible before). This is special case, when most recent snapshot
is not an automatic snapshot but an exported one (which was meant to be terminal, but was not due to failure).

Second case creates a dedicated Jet job which copies `IMap` contents using regular
`readMapP` and `writeMapP` processors.
They will not be extended to support `failOnIndeterminateOperationState` setting,
so it will still be possible that the exported snapshot can be silently corrupted.

### Rolling upgrade

Format of data in IMaps will not change.

### Performance

Most important for performance is snapshot taking as it occurs regularly.
Other processes are either manual or occur after error or topology changes
so are rare with little impact for overall performance.

In happy-path, when the cluster is stable, there are no additional `IMap` operations when taking snapshot.
Only in case of concurrent modification of `JobExecutionRecord` the `IMap` update can be repeated.
Other than that, there are no additional operations.
Jet already uses 1 sync backup for snapshot and other `IMap`s by default
and operations wait for backup ack before completing.

When the cluster is unstable, snapshot will take almost the same time
but may fail instead silently being successful with risk of corruption.

Snapshot restore has to ensure that `JobExecutionRecord` is safe.
This is piggybacked on `JobExecutionRecord` update already made when job starts/restarts.
This update will be changed to safe version.

`writeJobExecutionRecordSafe` can invoke `IMap` update a few times in case of concurrent `JobExecutionRecord` updates.
This increases number of `IMap` operations but is unlikely.