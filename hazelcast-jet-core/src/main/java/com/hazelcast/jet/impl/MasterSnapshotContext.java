/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.impl.JobExecutionRecord.SnapshotStats;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.operation.SnapshotOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation.SnapshotOperationResult;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.Operation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.impl.JobRepository.EXPORTED_SNAPSHOTS_PREFIX;
import static com.hazelcast.jet.impl.JobRepository.exportedSnapshotMapName;
import static com.hazelcast.jet.impl.JobRepository.snapshotDataMapName;
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

    /**
     * The queue with snapshots to run. An item is added to it regularly (to do
     * a regular snapshot) or when a snapshot export is requested by the user.
     *
     * The tuple contains:<ul>
     *     <li>{@code snapshotMapName}: user-specified name of the snapshot or
     *         null, if no name is specified
     *     <li>{@code isTerminal}: if true, execution will be terminated after the
     *         snapshot
     *     <li>{@code future}: future, that will be completed when the snapshot
     *         is validated.
     * </ul>
     *
     * Queue is accessed only in synchronized code.
     */
    private final Queue<Tuple3<String, Boolean, CompletableFuture<Void>>> snapshotQueue = new LinkedList<>();

    MasterSnapshotContext(MasterContext masterContext, ILogger logger) {
        mc = masterContext;
        this.logger = logger;
    }

    void enqueueSnapshot(String snapshotMapName, boolean isTerminal, CompletableFuture<Void> future) {
        snapshotQueue.add(tuple3(snapshotMapName, isTerminal, future));
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
            snapshotQueue.add(tuple3(null, false, null));
        } finally {
            mc.unlock();
        }
        tryBeginSnapshot();
    }

    void tryBeginSnapshot() {
        boolean isTerminal;
        String snapshotMapName;
        CompletableFuture<Void> future;
        mc.lock();
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

            Tuple3<String, Boolean, CompletableFuture<Void>> requestedSnapshot = snapshotQueue.poll();
            if (requestedSnapshot == null) {
                return;
            }
            snapshotInProgress = true;
            snapshotMapName = requestedSnapshot.f0();
            isTerminal = requestedSnapshot.f1();
            future = requestedSnapshot.f2();
            mc.jobExecutionRecord().startNewSnapshot(snapshotMapName);
        } finally {
            mc.unlock();
        }

        mc.writeJobExecutionRecord(false);
        long newSnapshotId = mc.jobExecutionRecord().ongoingSnapshotId();
        boolean isExport = snapshotMapName != null;
        String finalMapName = isExport ? exportedSnapshotMapName(snapshotMapName)
                : snapshotDataMapName(mc.jobId(), mc.jobExecutionRecord().ongoingDataMapIndex());
        mc.nodeEngine().getHazelcastInstance().getMap(finalMapName).clear();
        logger.info(String.format("Starting snapshot %d for %s", newSnapshotId, mc.jobIdString())
                + (isTerminal ? ", terminal" : "")
                + (isExport ? ", exporting to '" + snapshotMapName + '\'' : ""));

        Function<ExecutionPlan, Operation> factory =
                plan -> new SnapshotOperation(mc.jobId(), mc.executionId(), newSnapshotId, finalMapName, isTerminal);

        // Need to take a copy of executionId: we don't cancel the scheduled task when the execution
        // finalizes. If a new execution is started in the meantime, we'll use the execution ID to detect it.
        long localExecutionId = mc.executionId();
        mc.invokeOnParticipants(
                factory,
                responses -> mc.coordinationService().submitToCoordinatorThread(() ->
                        onSnapshotCompleted(responses, localExecutionId, newSnapshotId, finalMapName, isExport, isTerminal,
                                future)),
                null, true);
    }

    private void onSnapshotCompleted(
            Collection<Object> responses,
            long executionId,
            long snapshotId,
            String snapshotMapName,
            boolean wasExport,
            boolean wasTerminal,
            @Nullable CompletableFuture<Void> future
    ) {
        // Note: this method can be called after finalizeJob() is called or even after new execution started.
        // We only wait for snapshot completion if the job completed with a terminal snapshot and the job
        // was successful.
        SnapshotOperationResult mergedResult = new SnapshotOperationResult();
        for (Object response : responses) {
            // the response is either SnapshotOperationResult or an exception, see #invokeOnParticipants() method
            if (response instanceof Throwable) {
                response = new SnapshotOperationResult(0, 0, 0, (Throwable) response);
            }
            mergedResult.merge((SnapshotOperationResult) response);
        }

        IMap<Object, Object> snapshotMap = mc.nodeEngine().getHazelcastInstance().getMap(snapshotMapName);
        try {
            SnapshotValidationRecord validationRecord = new SnapshotValidationRecord(snapshotId,
                    mergedResult.getNumChunks(), mergedResult.getNumBytes(),
                    mc.jobExecutionRecord().ongoingSnapshotStartTime(), mc.jobId(), mc.jobName(),
                    mc.jobRecord().getDagJson());
            Object oldValue = snapshotMap.put(SnapshotValidationRecord.KEY, validationRecord);
            if (snapshotMapName.startsWith(EXPORTED_SNAPSHOTS_PREFIX)) {
                String snapshotName = snapshotMapName.substring(EXPORTED_SNAPSHOTS_PREFIX.length());
                mc.jobRepository().cacheValidationRecord(snapshotName, validationRecord);
            }
            if (oldValue != null) {
                logger.severe("SnapshotValidationRecord overwritten after writing to '" + snapshotMapName + "' for "
                        + mc.jobIdString() + ": snapshot data might be corrupted");
            }
        } catch (Exception e) {
            mergedResult.merge(new SnapshotOperationResult(0, 0, 0, e));
        }

        boolean isSuccess = mergedResult.getError() == null;
        if (!isSuccess) {
            logger.warning(mc.jobIdString() + " snapshot " + snapshotId + " failed on some member(s), " +
                    "one of the failures: " + mergedResult.getError());
            try {
                snapshotMap.clear();
            } catch (Exception e) {
                logger.warning(mc.jobIdString() + ": failed to clear snapshot map '" + snapshotMapName
                                + "' after a failure", e);
            }
        }
        SnapshotStats stats = mc.jobExecutionRecord().ongoingSnapshotDone(
                mergedResult.getNumBytes(), mergedResult.getNumKeys(), mergedResult.getNumChunks(),
                mergedResult.getError());
        mc.writeJobExecutionRecord(false);
        logger.info(String.format("Snapshot %d for %s completed with status %s in %dms, " +
                        "%,d bytes, %,d keys in %,d chunks, stored in '%s'",
                snapshotId, mc.jobIdString(), isSuccess ? "SUCCESS" : "FAILURE",
                stats.duration(), stats.numBytes(),
                stats.numKeys(), stats.numChunks(),
                snapshotMapName));
        if (!wasExport) {
            mc.jobRepository().clearSnapshotData(mc.jobId(), mc.jobExecutionRecord().ongoingDataMapIndex());
        }
        if (future != null) {
            if (isSuccess) {
                future.complete(null);
            } else {
                future.completeExceptionally(new JetException(mergedResult.getError()));
            }
        }

        mc.lock();
        try {
            if (mc.executionId() != executionId) {
                logger.fine("Not completing terminalSnapshotFuture on " + mc.jobIdString() + ", new execution " +
                        "already started, snapshot was for executionId=" + idToString(executionId));
                return;
            }
            assert snapshotInProgress : "snapshot not in progress";
            snapshotInProgress = false;
            if (wasTerminal) {
                // after a terminal snapshot, no more snapshots are scheduled in this execution
                boolean completedNow = terminalSnapshotFuture.complete(null);
                assert completedNow : "terminalSnapshotFuture was already completed";
                if (!isSuccess) {
                    // If the terminal snapshot failed, the executions might not terminate on some members
                    // normally and we don't care if it does - the snapshot is done, though unsuccessfully, and
                    // we have to bring the execution down.
                    // Let's execute the CompleteExecutionOperation to terminate them.
                    mc.jobContext().cancelExecutionInvocations(mc.jobId(), mc.executionId(), null);
                }
            } else if (!wasExport) {
                // if this snapshot was an automatic snapshot, schedule the next one
                mc.coordinationService().scheduleSnapshot(mc, executionId);
            }
        } finally {
            mc.unlock();
        }
        tryBeginSnapshot();
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
        for (Tuple3<String, Boolean, CompletableFuture<Void>> snapshotTuple : snapshotQueue) {
            if (snapshotTuple.f2() != null) {
                snapshotTuple.f2().completeExceptionally(
                        new JetException("Execution completed before snapshot executed"));
            }
        }
        snapshotQueue.clear();
    }

    public ILogger logger() {
        return logger;
    }
}
