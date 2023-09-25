/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl;

import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.JobExecutionRecord.SnapshotStats;
import com.hazelcast.jet.impl.exception.ExecutionNotFoundException;
import com.hazelcast.jet.impl.execution.SnapshotFlags;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.operation.SnapshotPhase1Operation;
import com.hazelcast.jet.impl.operation.SnapshotPhase1Operation.SnapshotPhase1Result;
import com.hazelcast.jet.impl.operation.SnapshotPhase2Operation;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.logging.Level;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.JobRepository.exportedSnapshotMapName;
import static com.hazelcast.jet.impl.JobRepository.safeImap;
import static com.hazelcast.jet.impl.JobRepository.snapshotDataMapName;
import static com.hazelcast.internal.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.Util.jobNameAndExecutionId;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Part of {@link MasterContext} that deals with snapshot creation.
 */
class MasterSnapshotContext {

    @SuppressWarnings("WeakerAccess") // accessed from subclass in jet-enterprise
    final MasterContext mc;
    private final ILogger logger;

    /**
     * It's true while a snapshot is in progress. It's used to prevent
     * concurrent snapshots.
     */
    private boolean snapshotInProgress;

    /**
     * A future (re)created when the job is started and completed when terminal
     * snapshot is completed (successfully or not).
     */
    @Nonnull
    private volatile CompletableFuture<Void> terminalSnapshotFuture = completedFuture(null);

    private class SnapshotRequest {
        /**
         * User-specified name of the snapshot or null, if no name is specified
         * (regular snapshot). This name is without
         * {@link JobRepository#EXPORTED_SNAPSHOTS_PREFIX} prefix.
         */
        final String snapshotName;
        /**
         * If true, execution will be terminated after the snapshot.
         */
        final boolean isTerminal;
        /**
         * Future, that will be completed when the snapshot is validated. Can
         * be null.
         */
        final CompletableFuture<Void> future;

        SnapshotRequest(@Nullable String snapshotName, boolean isTerminal, @Nullable CompletableFuture<Void> future) {
            this.snapshotName = snapshotName;
            this.isTerminal = isTerminal;
            this.future = future;
        }

        public boolean isExport() {
            return snapshotName != null;
        }

        /**
         * @see SnapshotFlags#isExportOnly(int)
         */
        public boolean isExportOnly() {
            return isExport() && !isTerminal;
        }

        public int snapshotFlags() {
            return SnapshotFlags.create(isTerminal, isExport());
        }

        public String mapName() {
            return isExport() ? exportedSnapshotMapName(snapshotName)
                    : snapshotDataMapName(mc.jobId(), mc.jobExecutionRecord().ongoingDataMapIndex());
        }

        /**
         * Complete snapshot future, if any.
         * @param error Error, or null for successful completion.
         */
        public void completeFuture(@Nullable Throwable error) {
            if (future != null) {
                if (error == null) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(error);
                }
            }
        }
    }

    /**
     * The queue with snapshots to run. An item is added to it regularly (to do
     * a regular snapshot) or when a snapshot export is requested by the user.
     * <p>
     * Queue is accessed only in synchronized code.
     */
    private final Queue<SnapshotRequest> snapshotQueue = new LinkedList<>();

    MasterSnapshotContext(MasterContext masterContext, ILogger logger) {
        mc = masterContext;
        this.logger = logger;
    }

    @SuppressWarnings("SameParameterValue") // used by jet-enterprise
    void enqueueSnapshot(String snapshotName, boolean isTerminal, CompletableFuture<Void> future) {
        snapshotQueue.add(new SnapshotRequest(snapshotName, isTerminal, future));
    }

    private void enqueueRegularSnapshot() {
        enqueueSnapshot(null, false, null);
    }

    void startScheduledSnapshot(long executionId) {
        mc.lock();
        try {
            if (mc.jobStatus() != RUNNING) {
                logger.fine("Not beginning snapshot, " + mc.jobIdString() + " is not RUNNING, but " + mc.jobStatus());
                return;
            }
            if (mc.executionId() != executionId) {
                // Current execution is completed and probably a new execution has started, but we don't
                // cancel the scheduled snapshot from previous execution, so let's just ignore it.
                logger.fine("Not beginning snapshot since unexpected execution ID received for " + mc.jobIdString()
                        + ". Received execution ID: " + idToString(executionId));
                return;
            }
            enqueueRegularSnapshot();
        } finally {
            mc.unlock();
        }
        tryBeginSnapshot();
    }

    void tryBeginSnapshot() {
        mc.coordinationService().submitToCoordinatorThread(() -> {
            final SnapshotRequest requestedSnapshot;
            mc.lock();
            long localExecutionId;
            try {
                if (mc.jobStatus() != RUNNING) {
                    logger.fine("Not beginning snapshot, " + mc.jobIdString() + " is not RUNNING, but " + mc.jobStatus());
                    return;
                }
                if (snapshotInProgress) {
                    logger.fine("Not beginning snapshot since one is already in progress " + mc.jobIdString());
                    return;
                }
                if (terminalSnapshotFuture.isDone()) {
                    logger.fine("Not beginning snapshot since terminal snapshot is already completed " + mc.jobIdString());
                    return;
                }

                requestedSnapshot = snapshotQueue.poll();
                if (requestedSnapshot == null) {
                    return;
                }
                snapshotInProgress = true;
                mc.jobExecutionRecord().startNewSnapshot(requestedSnapshot.snapshotName);
                localExecutionId = mc.executionId();
            } finally {
                mc.unlock();
            }

            long newSnapshotId = mc.jobExecutionRecord().ongoingSnapshotId();
            int snapshotFlags = requestedSnapshot.snapshotFlags();
            String mapName = requestedSnapshot.mapName();

            try {
                mc.writeJobExecutionRecordSafe(false);
                mc.nodeEngine().getHazelcastInstance().getMap(mapName).clear();
            } catch (Exception e) {
                logger.warning(String.format("Failed to start snapshot %d for %s",
                        newSnapshotId, jobNameAndExecutionId(mc.jobName(), localExecutionId)),
                        e);
                requestedSnapshot.completeFuture(e);
                return;
            }

            logFine(logger, "Starting snapshot %d for %s, flags: %s, writing to: %s",
                    newSnapshotId, jobNameAndExecutionId(mc.jobName(), localExecutionId),
                    SnapshotFlags.toString(snapshotFlags), requestedSnapshot.snapshotName);

            Function<ExecutionPlan, Operation> factory = plan ->
                    new SnapshotPhase1Operation(mc.jobId(), localExecutionId, newSnapshotId, mapName, snapshotFlags);

            // Need to take a copy of executionId: we don't cancel the scheduled task when the execution
            // finalizes. If a new execution is started in the meantime, we'll use the execution ID to detect it.
            mc.invokeOnParticipants(
                    factory,
                    responses -> onSnapshotPhase1Complete(responses, localExecutionId, newSnapshotId, requestedSnapshot),
                    null, true);
        });
    }

    /**
     * @param responses collected responses from the members
     */
    private void onSnapshotPhase1Complete(
            Collection<Map.Entry<MemberInfo, Object>> responses,
            long executionId,
            long snapshotId,
            SnapshotRequest requestedSnapshot
    ) {
        mc.coordinationService().submitToCoordinatorThread(() -> {
            SnapshotPhase1Result mergedResult = new SnapshotPhase1Result();
            List<CompletableFuture<Void>> missingResponses = new ArrayList<>();
            for (Map.Entry<MemberInfo, Object> entry : responses) {
                // the response is either SnapshotOperationResult or an exception, see #invokeOnParticipants() method
                Object response = entry.getValue();
                if (response instanceof Throwable) {
                    // If the member doesn't know the execution, it might have completed normally or exceptionally.
                    // If normally, we ignore it, if exceptionally, we'll also fail the snapshot. To know, we have
                    // to look at the result of the StartExecutionOperation, which might not have arrived yet. We'll collect
                    // all the responses to an array, and we'll wait for them later.
                    if (response instanceof ExecutionNotFoundException) {
                        missingResponses.add(mc.startOperationResponses().get(entry.getKey().getAddress()));
                        continue;
                    }
                    response = new SnapshotPhase1Result(0, 0, 0, (Throwable) response);
                }
                mergedResult.merge((SnapshotPhase1Result) response);
            }
            if (!missingResponses.isEmpty()) {
                LoggingUtil.logFine(logger, "%s will wait for %d responses to StartExecutionOperation in " +
                        "onSnapshotPhase1Complete()", mc.jobIdString(), missingResponses.size());
            }

            // In a typical case `missingResponses` will be empty. It will be non-empty if some member completed
            // its execution and some other did not, or near the completion of a job, e.g. after a failure.
            // `allOf` for an empty array returns a completed future immediately.
            // Another edge case is that we'll be waiting for a response to start operation from a next execution,
            // which can happen much later - we could handle it, but we ignore it: when it arrives, we'll find a
            // changed executionId and ignore the response. It also doesn't occupy a thread - we're using a future.
            CompletableFuture.allOf(missingResponses.toArray(new CompletableFuture[0]))
                    .whenComplete(withTryCatch(logger, (r, t) ->
                            onSnapshotPhase1CompleteWithStartResponses(responses, executionId, snapshotId, requestedSnapshot,
                                    mergedResult, missingResponses)));
        });
    }

    private void onSnapshotPhase1CompleteWithStartResponses(
            Collection<Entry<MemberInfo, Object>> responses,
            long executionId,
            long snapshotId,
            SnapshotRequest requestedSnapshot,
            SnapshotPhase1Result mergedResult,
            List<CompletableFuture<Void>> missingResponses
    ) {
        mc.coordinationService().submitToCoordinatorThread(() -> {
            final boolean isSuccess;
            boolean skipPhase2 = false;
            SnapshotStats stats;

            mc.lock();
            try {
                if (!missingResponses.isEmpty()) {
                    LoggingUtil.logFine(logger, "%s all awaited responses to StartExecutionOperation received or " +
                            "were already received", mc.jobIdString());
                }
                // Note: this method can be called after finalizeJob() is called or even after new execution started.
                // Check the execution ID to check if a new execution didn't start yet.
                if (executionId != mc.executionId()) {
                    LoggingUtil.logFine(logger, "%s: ignoring responses for snapshot %s phase 1: " +
                                    "the responses are from a different execution: %s. Responses: %s",
                            mc.jobIdString(), snapshotId, idToString(executionId), responses);
                    // a new execution started, ignore this response.
                    return;
                }

                for (CompletableFuture<Void> response : missingResponses) {
                    assert response.isDone() : "response not done";
                    try {
                        response.get();
                    } catch (ExecutionException e) {
                        mergedResult.merge(new SnapshotPhase1Result(0, 0, 0, e.getCause()));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                String mapName = requestedSnapshot.mapName();
                // Snapshot IMap proxy instance may be shared, but we always want it
                // to have failOnIndeterminateOperationState enabled.
                IMap<Object, Object> snapshotMap = safeImap(mc.nodeEngine().getHazelcastInstance().getMap(mapName));
                try {
                    SnapshotValidationRecord validationRecord = new SnapshotValidationRecord(snapshotId,
                            mergedResult.getNumChunks(), mergedResult.getNumBytes(),
                            mc.jobExecutionRecord().ongoingSnapshotStartTime(), mc.jobId(), mc.jobName(),
                            mc.jobRecord().getDagJson());

                    // The decision moment for _exported_ snapshots: after this the snapshot is valid to be restored
                    // from, however it will be not listed by JetInstance.getJobStateSnapshots unless the validation
                    // record is inserted into the cache below.
                    //
                    // Error during update for JobExecutionRecord does not invalidate the _exported_ snapshot.
                    // JobExecutionRecord data in IMap becomes stale (indicates that the exported snapshot is in progress)
                    // but it should not cause problems. They may be overwritten later (in-memory values will be correct)
                    // or ignored when JobExecutionRecord is loaded from IMap.
                    //
                    // Terminal exported snapshot is formally valid from this point on, but it is safe to use it
                    // to restore from only after and only if the job was cleanly terminated due to _this_ snapshot request.
                    // On API level, using this snapshot is not safe if cancelAndExportSnapshot throws exception
                    // and the job will not be cancelled but restarted.
                    Object oldValue = snapshotMap.put(SnapshotValidationRecord.KEY, validationRecord);

                    if (requestedSnapshot.isExport()) {
                        assert requestedSnapshot.snapshotName != null;
                        // update also for failed snapshots because the map may have contained different snapshot before
                        mc.jobRepository().cacheValidationRecord(requestedSnapshot.snapshotName, validationRecord);
                    }
                    if (oldValue != null) {
                        logger.severe("SnapshotValidationRecord overwritten after writing to '" + mapName
                                + "' for " + mc.jobIdString() + ": snapshot data might be corrupted");
                    }
                } catch (Exception e) {
                    mergedResult.merge(new SnapshotPhase1Result(0, 0, 0, e));
                }

                isSuccess = mergedResult.getError() == null;
                // update snapshot state in memory after success or failure
                stats = mc.jobExecutionRecord().ongoingSnapshotDone(
                        mergedResult.getNumBytes(), mergedResult.getNumKeys(), mergedResult.getNumChunks(),
                        mergedResult.getError(), requestedSnapshot.isTerminal);

                // There is no need to restart job in case of a failed snapshot:
                // - ongoingSnapshotId is safe in IMap, because it was written at the beginning
                // - snapshotId was not updated
                // - we can roll back transactions now (2nd phase) if needed, we would do it anyway after restart
                // So regardless of whether JobExecutionRecord in IMap turns out to be new or old,
                // the state will be consistent.
                //
                // There is no need to restart job/skip phase2 in case of exported _non-terminal_ snapshot,
                // because it writes to separate IMap and data in it should be safe by now.
                // JobExecutionRecord.ongoingSnapshotDone does not update critical data in case of
                // _non-terminal_ exported snapshots.
                if (isSuccess && !requestedSnapshot.isExportOnly()) {
                    try {
                        // The decision moment for regular snapshots: after this the snapshot is ready to be restored from.
                        // The decision moment also for terminal exported snapshot: after this the
                        // job can be cleanly terminated without any prepared transactions in unknown state,
                        // and the snapshot can be used to safely start a new job.
                        mc.writeJobExecutionRecordSafe(false);
                    } catch (IndeterminateOperationStateException indeterminate) {
                        skipPhase2 = true;
                        logger.warning(mc.jobIdString() + " snapshot " + snapshotId + " update of JobExecutionRecord " +
                                "was indeterminate. Will restart the job forcefully.", indeterminate);
                    } catch (Exception otherError) {
                        skipPhase2 = true;
                        logger.warning(mc.jobIdString() + " snapshot " + snapshotId +
                                " update of JobExecutionRecord failed. Will restart the job forcefully.", otherError);
                    }
                } else {
                    mc.writeJobExecutionRecord(false);
                }

                if (logger.isFineEnabled()) {
                    logger.fine(String.format("Snapshot %d phase 1 for %s completed with status %s in %dms, " +
                                    "%,d bytes, %,d keys in %,d chunks, stored in '%s'%s",
                            snapshotId, mc.jobIdString(),
                            (skipPhase2 ? "INDETERMINATE/" : "") + (isSuccess ? "SUCCESS" : "FAILURE"),
                            stats.duration(), stats.numBytes(), stats.numKeys(), stats.numChunks(),
                            mapName,
                            (skipPhase2 ? ", skipping " : ", proceeding to ") + "phase 2"));
                }

                if (!isSuccess) {
                    logger.warning(mc.jobIdString() + " snapshot " + snapshotId + " phase 1 failed on some " +
                            "member(s), one of the failures: " + mergedResult.getError());
                    try {
                        // Clear data of failed snapshot (automatic or exported) to decrease memory usage.
                        // This can be done regardless of skipPhase2 because failed snapshot
                        // can never be used for restore.
                        snapshotMap.clear();
                    } catch (Exception e) {
                        logger.warning(mc.jobIdString() + ": failed to clear snapshot map '" + mapName
                                + "' after a failure", e);
                    }
                }

                // Do not clear snapshot data when JobExecutionRecord update was indeterminate.
                // It may turn out that this will be a correct snapshot after all.
                if (isSuccess && !skipPhase2 && !requestedSnapshot.isExport()) {
                    // clear IMap for next automatic snapshot early to decrease memory usage
                    mc.jobRepository().clearSnapshotData(mc.jobId(), mc.jobExecutionRecord().ongoingDataMapIndex());
                }
            } finally {
                mc.unlock();
            }


            if (skipPhase2) {
                // Restart the job without performing phase 2 now.
                // Commit/rollback will be performed during restore from snapshot.
                //
                // This snapshot could have been a terminal snapshot (suspend, cancel_graceful).
                // In such case the job will also be restarted, so we do not keep prepared transactions
                // in external sources and sinks in prepared state for a long time.
                // If needed, user should repeat the operation that initiated the snapshot.
                //
                // This snapshot (automatic or manual) may have been queued before later termination request
                // (which may be graceful or forceful).
                // In such case job fails on first snapshot and termination request is ignored.
                // That is because termination request may want named snapshot, and we could be performing automatic.
                TerminationMode newMode = TerminationMode.RESTART_FORCEFUL;
                logger().fine(mc.jobIdString() + ": Terminating job without performing snapshot phase 2 with mode " + newMode);
                // we do not have mc.lock() here and requestTermination() may be invoked concurrently
                // so do not use it. Instead, fail execution on members.
                mc.jobContext().handleTermination(newMode);
                requestedSnapshot.completeFuture(new JetException("Snapshot in unknown state"));
            } else {
                // start the phase 2
                Function<ExecutionPlan, Operation> factory = plan -> new SnapshotPhase2Operation(
                        mc.jobId(), executionId, snapshotId, isSuccess && !requestedSnapshot.isExportOnly());
                mc.invokeOnParticipants(factory,
                        responses2 -> onSnapshotPhase2Complete(mergedResult.getError(), responses2, executionId,
                                snapshotId, requestedSnapshot, stats.startTime()),
                        null, true);
            }
        });
    }

    /**
     * @param phase1Error error from the phase-1. Null if phase-1 was successful.
     * @param responses collected responses from the members
     * @param startTime phase-1 start time
     */
    private void onSnapshotPhase2Complete(
            String phase1Error,
            Collection<Entry<MemberInfo, Object>> responses,
            long executionId,
            long snapshotId,
            SnapshotRequest requestedSnapshot,
            long startTime
    ) {
        mc.coordinationService().submitToCoordinatorThread(() -> {
            if (executionId != mc.executionId()) {
                LoggingUtil.logFine(logger, "%s: ignoring responses for snapshot %s phase 2: " +
                                "the responses are from a different execution: %s. Responses: %s",
                        mc.jobIdString(), snapshotId, idToString(executionId), responses);
                return;
            }

            for (Entry<MemberInfo, Object> response : responses) {
                if (response.getValue() instanceof Throwable) {
                    logger.log(
                            response.getValue() instanceof ExecutionNotFoundException ? Level.FINE : Level.WARNING,
                            SnapshotPhase2Operation.class.getSimpleName() + " for snapshot " + snapshotId + " in "
                            + mc.jobIdString() + " failed on member: " + response, (Throwable) response.getValue());
                }
            }

            requestedSnapshot.completeFuture(phase1Error == null ? null : new JetException(phase1Error));

            mc.lock();
            try {
                // double-check the execution ID after locking
                if (executionId != mc.executionId()) {
                    logger.fine("Not completing terminalSnapshotFuture on " + mc.jobIdString() + ", new execution " +
                            "already started, snapshot was for executionId=" + idToString(executionId));
                    return;
                }
                assert snapshotInProgress : "snapshot not in progress";
                snapshotInProgress = false;
                if (requestedSnapshot.isTerminal) {
                    // after a terminal snapshot, no more snapshots are scheduled in this execution
                    boolean completedNow = terminalSnapshotFuture.complete(null);
                    assert completedNow : "terminalSnapshotFuture was already completed";
                    if (phase1Error != null) {
                        // If the terminal snapshot failed, the executions might not terminate on some members
                        // normally and we don't care if they do - the snapshot is done and we have to bring the
                        // execution down. Let's execute the CompleteExecutionOperation to terminate them.
                        mc.jobContext().cancelExecutionInvocations(mc.jobId(), mc.executionId(), null, null);
                    }
                } else if (!requestedSnapshot.isExport()) {
                    // if this snapshot was an automatic snapshot, schedule the next one
                    mc.coordinationService().scheduleSnapshot(mc, executionId);
                }
            } finally {
                mc.unlock();
            }
            if (logger.isFineEnabled()) {
                logger.fine("Snapshot " + snapshotId + " for " + mc.jobIdString() + " completed in "
                        + (System.currentTimeMillis() - startTime) + "ms, status="
                        + (phase1Error == null ? "success" : "failure: " + phase1Error));
            }

            tryBeginSnapshot();
        });
    }

    CompletableFuture<Void> terminalSnapshotFuture() {
        return terminalSnapshotFuture;
    }

    void onExecutionStarted() {
        snapshotInProgress = false;
        assert snapshotQueue.isEmpty() : "snapshotQueue not empty";
        terminalSnapshotFuture = new CompletableFuture<>();
    }

    void onExecutionTerminated() {
        for (SnapshotRequest snapshotRequest : snapshotQueue) {
            snapshotRequest.completeFuture(new JetException("Execution completed before snapshot executed"));
        }
        snapshotQueue.clear();
    }

    public ILogger logger() {
        return logger;
    }
}
