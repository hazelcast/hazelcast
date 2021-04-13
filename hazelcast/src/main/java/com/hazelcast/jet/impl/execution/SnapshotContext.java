/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.operation.SnapshotPhase1Operation;
import com.hazelcast.jet.impl.operation.SnapshotPhase1Operation.SnapshotPhase1Result;
import com.hazelcast.jet.impl.operation.SnapshotPhase2Operation;
import com.hazelcast.logging.ILogger;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class SnapshotContext {

    private final ILogger logger;

    private final String jobNameAndExecutionId;
    private final ProcessingGuarantee guarantee;

    /**
     * Flags of the last begun snapshot.
     */
    private volatile int snapshotFlags;

    /**
     * Current number of {@link StoreSnapshotTasklet}s in the job. It's
     * decremented as the tasklets complete (this is when they receive
     * DONE_ITEM and after all pending async ops completed).
     */
    private int numSsTasklets = Integer.MIN_VALUE;

    /**
     * Current number of {@link ProcessorTasklet}s in the job. It's
     * decremented as the tasklets complete.
     */
    private int numPTasklets = Integer.MIN_VALUE;

    /**
     * Current number of {@link StoreSnapshotTasklet}s in the job connected
     * to higher priority vertices. It's decremented when higher priority
     * tasklet completes. Snapshot is postponed until this counter is 0.
     */
    private int numPrioritySsTasklets = Integer.MIN_VALUE;

    /**
     * Used to count remaining participants in both phase 1 and 2 of the
     * snapshot. In phase 1 it's the remaining number of {@link
     * StoreSnapshotTasklet}, in phase 2 it's the number of remaining {@link
     * ProcessorTasklet}s. When it is decremented to 0, the phase is complete
     * and the member replies to the master to start the next phase or next
     * snapshot.
     * <p>
     * It's in the range 0..numSsTasklets (inclusive) or 0..numPTasklets.
     */
    private final AtomicInteger numRemainingTasklets = new AtomicInteger();

    /**
     * Holder for the snapshot error, if any
     */
    private final AtomicReference<Throwable> snapshotError = new AtomicReference<>();

    /**
     * Snapshot id of current active snapshot. Source processors read
     * it and when they see higher value, they start a snapshot with that
     * ID. {@code -1} means no snapshot was ever started for this job.
     */
    private volatile long activeSnapshotIdPhase1;

    /**
     * Snapshot id of current active snapshot. All processors read it and when
     * they see higher value, they call {@link Processor#snapshotCommitFinish}.
     */
    private volatile long activeSnapshotIdPhase2;

    /**
     * Success state for the phase 2 of the snapshot.
     */
    private volatile boolean lastPhase1Successful;

    /**
     * The snapshotId of the snapshot that should be performed. It's equal to
     * {@link #activeSnapshotIdPhase1} most of the time, except for the case when the
     * snapshot was postponed due to higher priority tasklets still running, in
     * which case it's larger by 1.
     */
    private long currentSnapshotId;

    /**
     * The data map name the active snapshot will be written to. If it's null,
     * we can't write the snapshot - we have to learn the name from the master
     * first.
     */
    private volatile String currentMapName;

    /**
     * Future which will be created when phase 1 starts and completed and
     * nulled out when the phase 1 completes.
     */
    private volatile CompletableFuture<SnapshotPhase1Result> phase1Future;

    /**
     * Future which will be created when phase 2 starts and completed and
     * nulled out when the phase 2 completes.
     */
    private volatile CompletableFuture<Void> phase2Future;

    private final AtomicLong totalBytes = new AtomicLong();
    private final AtomicLong totalKeys = new AtomicLong();
    private final AtomicLong totalChunks = new AtomicLong();
    private boolean isCancelled;

    public SnapshotContext(ILogger logger, String jobNameAndExecutionId, long activeSnapshotId,
                           ProcessingGuarantee guarantee
    ) {
        this.jobNameAndExecutionId = jobNameAndExecutionId;
        this.activeSnapshotIdPhase1 = activeSnapshotIdPhase2 = currentSnapshotId = activeSnapshotId;
        this.guarantee = guarantee;
        this.logger = logger;
    }

    /**
     * Id of the last started snapshot, phase 1.
     */
    long activeSnapshotIdPhase1() {
        return activeSnapshotIdPhase1;
    }

    /**
     * Id of the last started snapshot, phase 2.
     */
    long activeSnapshotIdPhase2() {
        return activeSnapshotIdPhase2;
    }

    public long currentSnapshotId() {
        return currentSnapshotId;
    }

    /**
     * Returns the name of the map that the current snapshot should be written
     * to.
     */
    public String currentMapName() {
        return currentMapName;
    }

    boolean isTerminalSnapshot() {
        return SnapshotFlags.isTerminal(snapshotFlags);
    }

    public boolean isExportOnly() {
        return SnapshotFlags.isExportOnly(snapshotFlags);
    }

    boolean isLastPhase1Successful() {
        return lastPhase1Successful;
    }

    public ProcessingGuarantee processingGuarantee() {
        return guarantee;
    }

    synchronized void initTaskletCount(int numPTasklets, int numSsTasklets, int numPrioritySsTasklets) {
        assert this.numSsTasklets == Integer.MIN_VALUE && this.numPTasklets == Integer.MIN_VALUE
                : "Tasklet count already set";
        assert numSsTasklets >= 1
                && numPrioritySsTasklets >= 0
                && numSsTasklets >= numPrioritySsTasklets
                && numPTasklets >= numSsTasklets
                : "numPTasklets=" + numPTasklets + ", numSsTasklets=" + numSsTasklets + ", numPrioritySsTasklets="
                        + numPrioritySsTasklets;

        this.numSsTasklets = numSsTasklets;
        this.numPTasklets = numPTasklets;
        this.numPrioritySsTasklets = numPrioritySsTasklets;
    }

    /**
     * This method is called when the member received {@link
     * SnapshotPhase1Operation}.
     */
    synchronized CompletableFuture<SnapshotPhase1Result> startNewSnapshotPhase1(
            long snapshotId, String mapName, int flags) {
        if (snapshotId == currentSnapshotId) {
            // This is possible when a SnapshotOperation is retried. We will throw because we
            // don't know the result of the previous snapshot (it may have failed) and this is rare
            // if not impossible.
            throw new RuntimeException("new snapshotId equal to previous, operation probably retried. Previous="
                    + currentSnapshotId + ", new=" + snapshotId);
        }
        assert snapshotId == currentSnapshotId + 1
                : "New snapshotId for " + jobNameAndExecutionId + " not incremented by 1. " +
                        "Previous=" + currentSnapshotId + ", new=" + snapshotId;
        assert currentSnapshotId == activeSnapshotIdPhase1 : "last snapshot was postponed but not started";
        assert numSsTasklets >= 0 : "numSsTasklets=" + numSsTasklets;
        assert phase1Future == null : "phase 1 already in progress";
        assert phase2Future == null : "phase 2 still ongoing";
        assert snapshotId == activeSnapshotIdPhase2 + 1
                : "snapshotId=" + snapshotId + ", activeSnapshotIdPhase2=" + activeSnapshotIdPhase2;
        if (isCancelled) {
            throw new CancellationException("execution cancelled");
        }
        this.snapshotFlags = flags;

        boolean success = numRemainingTasklets.compareAndSet(0, numSsTasklets);
        assert success : "numRemainingTasklets wasn't 0, but " + numRemainingTasklets.get();

        currentSnapshotId = snapshotId;
        currentMapName = mapName;

        if (numPrioritySsTasklets == 0) {
            // if there are no higher priority tasklets, start the snapshot immediately
            activeSnapshotIdPhase1 = currentSnapshotId;
        } else {
            // the snapshot will be started once all higher priority sources are done
            // see #taskletDone()
            logger.info("Snapshot " + snapshotId + " for " + jobNameAndExecutionId + " is postponed" +
                    " until all higher priority vertices are completed (number of such vertices = "
                    + numPrioritySsTasklets + ')');
        }
        if (numSsTasklets == 0) {
            // member is already done with the job and master didn't know it yet - we are immediately successful
            return completedFuture(new SnapshotPhase1Result(0, 0, 0, null));
        }
        phase1Future = new CompletableFuture<>();
        return phase1Future;
    }

    /**
     * This method is called when the member received {@link
     * SnapshotPhase2Operation}.
     */
    synchronized CompletableFuture<Void> startNewSnapshotPhase2(long snapshotId, boolean success) {
        if (snapshotId == activeSnapshotIdPhase2) {
            // this is possible in case when the operation packet was lost and retried
            logger.warning("Second request for phase 2 for snapshot " + snapshotId);
            CompletableFuture<Void> res = this.phase2Future;
            if (res == null) {
                // if there's no ongoing phase 2, we don't know what was the response. We reply with
                // 100% success. For regular snapshots the failures are ignored. They are reported to
                // the user only for exported snapshot. We neglect that.
                res = completedFuture(null);
            }
            return res;
        }
        assert snapshotId == activeSnapshotIdPhase1 : "requested phase 2 for snapshot ID " + snapshotId
                + ", but phase 1 snapshot ID is " + activeSnapshotIdPhase1;
        assert phase1Future == null : "phase 1 still ongoing";
        assert phase2Future == null : "phase 2 already in progress";
        assert snapshotId > activeSnapshotIdPhase2
                : "new snapshotId for phase 2 not larger than previous. Previous=" + activeSnapshotIdPhase2 + ", new="
                        + snapshotId;
        assert numPTasklets >= 0 : "numPTasklets=" + numPTasklets;
        if (isCancelled) {
            throw new CancellationException("execution cancelled");
        }
        this.lastPhase1Successful = success;
        assert numPrioritySsTasklets == 0 : "numPrioritySsTasklets=" + numPrioritySsTasklets;

        boolean casSuccess = numRemainingTasklets.compareAndSet(0, numPTasklets);
        assert casSuccess : "numRemainingTasklets wasn't 0, but " + numRemainingTasklets.get();
        activeSnapshotIdPhase2 = snapshotId;
        if (numPTasklets == 0) {
            // member is already done with the job and master didn't know it yet - we are immediately successful
            return completedFuture(null);
        }
        phase2Future = new CompletableFuture<>();
        return phase2Future;
    }

    /**
     * Called when {@link StoreSnapshotTasklet} is done (received DONE_ITEM
     * from all its processors).
     *
     * @param lastCompletedSnapshotId id of the last snapshot completed by the
     *                               tasklet used to determine, whether the
     *                               tasklet did or did not do the current
     *                               snapshot.
     */
    synchronized void storeSnapshotTaskletDone(long lastCompletedSnapshotId, boolean isHigherPrioritySource) {
        assert numSsTasklets > 0 : "numSsTasklets=" + numSsTasklets;
        assert lastCompletedSnapshotId <= activeSnapshotIdPhase1 : "activeSnapshotIdPhase1=" + activeSnapshotIdPhase1
                + ", tasklet.lastCompletedSnapshotId=" + lastCompletedSnapshotId;

        numSsTasklets--;
        if (isHigherPrioritySource) {
            assert numPrioritySsTasklets > 0 : "numPrioritySsTasklets=" + numPrioritySsTasklets;
            numPrioritySsTasklets--;
            // after all higher priority vertices are done we can start the snapshot
            if (numPrioritySsTasklets == 0 && activeSnapshotIdPhase1 < currentSnapshotId) {
                activeSnapshotIdPhase1 = currentSnapshotId;
                logger.info("Postponed snapshot " + activeSnapshotIdPhase1 + " for " + jobNameAndExecutionId
                        + " started");
            }
        }
        assert numPrioritySsTasklets <= numSsTasklets : "numPrioritySsTasklets > numSsTasklets";
        assert lastCompletedSnapshotId <= currentSnapshotId : "tasklet completed a snapshot that didn't start yet";

        if (lastCompletedSnapshotId < currentSnapshotId) {
            // if tasklet is done before it was aware of the current snapshot, we
            // treat it as if it already completed the snapshot without any data
            phase1DoneForTasklet(0, 0, 0);
        } else {
            assert lastCompletedSnapshotId == activeSnapshotIdPhase1
                            && lastCompletedSnapshotId == activeSnapshotIdPhase2
                    : "lastCompletedSnapshotId=" + lastCompletedSnapshotId + ", activeSnapshotIdPhase1="
                            + activeSnapshotIdPhase1 + ", activeSnapshotIdPhase2=" + activeSnapshotIdPhase2;
        }
    }

    synchronized void processorTaskletDone() {
        assert numPTasklets > 0 : "numPTasklets=" + numPTasklets;
        numPTasklets--;
    }

    /**
     * Called when phase 1 of current snapshot is done in {@link
     * StoreSnapshotTasklet} (it received barriers from all its processors and
     * all async flush operations are done).
     */
    void phase1DoneForTasklet(long numBytes, long numKeys, long numChunks) {
        totalBytes.addAndGet(numBytes);
        totalKeys.addAndGet(numKeys);
        totalChunks.addAndGet(numChunks);
        int newRemainingTasklets = numRemainingTasklets.decrementAndGet();
        assert newRemainingTasklets >= 0 : "newRemainingTasklets=" + newRemainingTasklets;
        if (newRemainingTasklets == 0) {
            handlePhase1Done();
        }
    }

    /**
     * Called when current snapshot is done in {@link StoreSnapshotTasklet}
     * (it received barriers from all its processors and all async flush
     * operations are done).
     */
    void phase2DoneForTasklet() {
        int newRemainingTasklets = numRemainingTasklets.decrementAndGet();
        assert newRemainingTasklets >= 0 : "newRemainingTasklets=" + newRemainingTasklets;
        if (newRemainingTasklets == 0) {
            handlePhase2Done();
        }
    }

    /**
     * Method is called when execution is cancelled/terminated. The existing
     * ongoing snapshot will be completed with failure and no more snapshots
     * will be allowed to start.
     */
    synchronized void cancel() {
        if (phase1Future != null) {
            reportError(new CancellationException("execution cancelled"));
            handlePhase1Done();
        }
        if (phase2Future != null) {
            handlePhase2Done();
        }
        isCancelled = true;
    }

    private synchronized void handlePhase1Done() {
        if (isCancelled) {
            assert phase1Future == null : "phase1Future=" + phase1Future;
            return;
        }
        phase1Future.complete(
                new SnapshotPhase1Result(totalBytes.get(), totalKeys.get(), totalChunks.get(), snapshotError.get()));

        phase1Future = null;
        snapshotError.set(null);
        totalBytes.set(0);
        totalKeys.set(0);
        totalChunks.set(0);
        currentMapName = null;
    }

    private synchronized void handlePhase2Done() {
        if (isCancelled) {
            assert phase2Future == null : "phase2Future=" + phase2Future;
            return;
        }
        phase2Future.complete(null);
        phase2Future = null;
    }

    void reportError(Throwable ex) {
        snapshotError.compareAndSet(null, ex);
    }

    // public-visible for tests
    AtomicInteger getNumRemainingTasklets() {
        return numRemainingTasklets;
    }
}
