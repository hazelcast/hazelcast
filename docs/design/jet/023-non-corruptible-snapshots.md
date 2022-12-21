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

## Current snapshotting algorithm

Jet uses [2-phase snapshot algorithm](https://hazelcast.com/blog/transactional-connectors-in-hazelcast-jet/)
which from the point of view of snapshot data consistency can be summarized as follows.

### Snapshot taking procedure

1. Initiate snapshot: generate new `snapshotId`, notify all processors
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

It is allowed for the snapshot to become corrupted if more than 1 nodes fail simultaneously,
per standard guarantees of `IMap`. In such case exactly-once semantics is not guaranteed.
This could be alleviated by increasing number of sync backups of `IMap`
but is outside the scope of this change. 

The change does not aim to protect against "zombie" operations
(operations executed very late by repetition logic).

### IMap changes

`IMap` modifying operations will throw `IndeterminateOperationStateException` 
after `hazelcast.operation.backup.timeout.millis` if not all backups have been acked in time.
Currently, in such case `IMap` operations return success.
This behavior will be configurable in `MapConfig`.

`IndeterminateOperationStateException` will be enabled for snapshot data `IMap`.

TBD if it should be enabled for other Jet maps.

### Updated snapshotting algorithm

New algorithm uses the fact that some `IMap` operations can end with `IndeterminateOperationStateException`
("indeterminate result" in short).

#### Snapshot taking procedure

1. Clear ongoing snapshot map.
   Indeterminate result -> ignore or fail and start over (if clear was indeterminate, put also likely will be indeterminate)
2. Initiate snapshot: generate new `snapshotId`, notify all processors
3. 1st snapshot phase (`saveSnapshot` + `snapshotCommitPrepare`): 
   Each processor instance writes its state to `IMap` as "chunk".
   Any indeterminate put -> snapshot failed
4. Make decision: if all processors succeeded in 1st phase then the snapshot will be committed in 2nd phase,
   otherwise it will be rolled back.
5. Write `SnapshotVerificationRecord` (*).
   Indeterminate result -> restart job immediately without performing 2nd phase of snapshot
   (no rollback and no commit, one of them will happen after restore)
6. Update ongoing data map index in memory if the snapshot was successful.
7. TODO: Update JExR - maybe snapshot data will be removed from it. Indeterminate, other error -> ignore
8. Optionally - clear new ongoing snapshot map if the snapshot was successful. Indeterminate, other error -> ignore
9. 2nd snapshot phase (`snapshotCommitFinish`) with decision made earlier.
   This step can be performed concurrently with processing of next items and must ultimately succeed.
10. Schedule next snapshot

(*) We could repeat for a few times until we obtained different result than indeterminate (can be success or failure).
This does not block processing in processors but increases amount of uncommitted work that can be lost.
This is also more complicated in implementation and may be considered later if needed.

#### Snapshot restore procedure

1. Check both snapshot maps and find the newest snapshot (greater `snapshotId`) which:
   1. Has `SnapshotVerificationRecord` 
   2. Is not corrupted (check just in case)
2. It the snapshot was found:
   1. Write `SnapshotVerificationRecord` to map from which it was read to ensure that it is replicated.
      In case of indeterminate result - do not start job now, schedule restart later.
   2. Restore processors state from snapshot, rollback/commit any prepared transactions
   3. Set ongoing data map index to the other map
3. If the snapshot was not found:
   1. Rollback/commit any prepared transactions
4. Schedule next snapshot

#### Correctness

`SnapshotVerificationRecord` for given snapshot can be in one 3 states:
1. nonexistent
2. maybe safe (writing to IMap ended in indeterminate result, may exist on primary replica only or in primary and backups)
3. safe (in primary and backups)

Updated algorithm guarantees correctness by holding the following invariants:
1. Snapshot restore is never performed concurrently with snapshot taking
   (guaranteed by various mechanisms in Hazelcast and Jet, among others
   `MasterJobContext.scheduleRestartIfClusterIsNotSafe` and split-brain protection).
2. Existence of `SnapshotVerificationRecord` implies that given snapshot was prepared (1st phase) successfully.
   Existence of `SnapshotVerificationRecord` does not determine if 2nd phase has already completed or not. 
3. 2nd snapshot phase commit is executed if and only if `SnapshotVerificationRecord` is "safe".
4. 2nd snapshot phase rollback is executed if and only if `SnapshotVerificationRecord` is "nonexistent".
5. If `SnapshotVerificationRecord` was "maybe safe" but became "nonexistent"
   snapshot data was irrecoverably lost and the snapshot cannot ever be used. 
   The same `SnapshotVerificationRecord` cannot reappear. 
   In other words, transition from "nonexistent" to "safe" is not possible during restore.
6. Lack of `SnapshotVerificationRecord` during restore means that given snapshot failed 
   during preparation (1st phase) or writing `SnapshotVerificationRecord`. 
   There may exist some transaction that have to be rolled back.
7. It is possible to rollback transactions prepared by snapshot that failed. 
   This is implemented using [XA protocol or workarounds](https://hazelcast.com/blog/transactional-connectors-in-hazelcast-jet/).
8. Next snapshot is performed after 2nd phase of previous snapshot (commit or rollback) has completed.

### Other changes

TBD: Removal of snapshot data from `JobExecutionRecord`

## Impact

### Job suspension

TBD: job suspension creates snapshot. What if it is indeterminate? There are already some warnings in `Job.suspend()` docs. 

### Manual snapshots

Manually executed snapshots will benefit from added protection against corruption.
However, in case of unstable cluster, performing manual snapshot may cause job restart.

TBD: check if this is actually the case, because for manual snapshots 2nd phase is not executed (?)

### Performance

In happy-path, when the cluster is stable, nothing changes. 
Jet already uses 1 sync backup for snapshot `IMap`s and operations wait for backup response before completing.

If we skip updating snapshot data in `JobExecutionRecord` it may actually improve performance.

When the cluster is unstable, snapshot will take the same time
but may fail instead silently being successful with risk of corruption.

Snapshot restore has to look into 2 snapshot maps instead of 1, but usually newer one will be correct.
Difference is negligible (1 additional `IMap` lookup for `SnapshotVerificationRecord`)
and may be compensated by not having to check `JobExecutionRecord`.