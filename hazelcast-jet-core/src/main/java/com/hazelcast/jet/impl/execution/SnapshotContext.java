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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.operation.SnapshotOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation.SnapshotOperationResult;
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
     * If true, the job should terminate after the snapshot is processed.
     */
    private volatile boolean isTerminal;

    /**
     * Current number of {@link StoreSnapshotTasklet}s in the job. It's
     * decremented as the tasklets complete (this is when they receive
     * DONE_ITEM and after all pending async ops completed).
     */
    private int numTasklets = Integer.MIN_VALUE;

    /**
     * Current number of {@link StoreSnapshotTasklet}s in the job connected
     * to higher priority vertices. It's decremented when higher priority
     * tasklet completes. Snapshot is postponed until this counter is 0.
     */
    private int numHigherPriorityTasklets = Integer.MIN_VALUE;

    /**
     * Remaining number of {@link StoreSnapshotTasklet}s in currently created
     * snapshot. When it is decremented to 0, the snapshot is complete and new
     * one can start.
     * <p>
     * It's in the range 0..numTasklets (inclusive).
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
    private volatile long activeSnapshotId;

    /**
     * The snapshotId of the snapshot that should be performed. It's equal to
     * {@link #activeSnapshotId} most of the time, except for the case when the
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
     * Future which will be created when a snapshot starts and completed and
     * nulled out when the snapshot completes.
     */
    private volatile CompletableFuture<SnapshotOperationResult> future;

    private final AtomicLong totalBytes = new AtomicLong();
    private final AtomicLong totalKeys = new AtomicLong();
    private final AtomicLong totalChunks = new AtomicLong();
    private boolean isCancelled;

    public SnapshotContext(ILogger logger, String jobNameAndExecutionId, long activeSnapshotId,
                           ProcessingGuarantee guarantee
    ) {
        this.jobNameAndExecutionId = jobNameAndExecutionId;
        this.activeSnapshotId = currentSnapshotId = activeSnapshotId;
        this.guarantee = guarantee;
        this.logger = logger;
    }

    /**
     * Id of the last started snapshot
     */
    long activeSnapshotId() {
        return activeSnapshotId;
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
        return isTerminal;
    }

    ProcessingGuarantee processingGuarantee() {
        return guarantee;
    }

    synchronized void initTaskletCount(int totalTaskletCount, int highPriorityTaskletCount) {
        assert this.numTasklets == Integer.MIN_VALUE : "Tasklet count already set once.";
        assert totalTaskletCount >= highPriorityTaskletCount :
                "totalTaskletCount=" + totalTaskletCount + ", highPriorityTaskletCount=" + highPriorityTaskletCount;
        assert totalTaskletCount > 0 : "totalTaskletCount=" + totalTaskletCount;
        assert highPriorityTaskletCount >= 0 : "highPriorityTaskletCount=" + highPriorityTaskletCount;

        this.numTasklets = totalTaskletCount;
        this.numHigherPriorityTasklets = highPriorityTaskletCount;
    }

    /**
     * This method is called when the member received {@link
     * SnapshotOperation}.
     */
    synchronized CompletableFuture<SnapshotOperationResult> startNewSnapshot(
            long snapshotId, String mapName, boolean isTerminal) {
        if (snapshotId == currentSnapshotId) {
            // This is possible when a SnapshotOperation is retried. We will throw because we
            // don't know the result of the previous snapshot (it may have failed) and this is rare
            // if not impossible.
            throw new RuntimeException("new snapshotId equal to previous, operation possibly retried. Previous="
                    + currentSnapshotId + ", new=" + snapshotId);
        }
        assert snapshotId == currentSnapshotId + 1
                : "New snapshotId for " + jobNameAndExecutionId + " not incremented by 1. " +
                        "Previous=" + currentSnapshotId + ", new=" + snapshotId;
        assert currentSnapshotId == activeSnapshotId : "last snapshot was postponed but not started";
        assert numTasklets >= 0 : "numTasklets=" + numTasklets;
        if (isCancelled) {
            throw new CancellationException("execution cancelled");
        }
        this.isTerminal = isTerminal;

        boolean success = numRemainingTasklets.compareAndSet(0, numTasklets);
        assert success : "numRemainingTasklets wasn't 0, but " + numRemainingTasklets.get();

        currentSnapshotId = snapshotId;
        currentMapName = mapName;

        if (numHigherPriorityTasklets == 0) {
            // if there are no higher priority tasklets, start the snapshot immediately
            activeSnapshotId = currentSnapshotId;
        } else {
            // the snapshot will be started once all higher priority sources are done
            // see #taskletDone()
            logger.info("Snapshot " + snapshotId + " for " + jobNameAndExecutionId + " is postponed" +
                    " until all higher priority vertices are completed (number of such vertices = "
                    + numHigherPriorityTasklets + ')');
        }
        if (numTasklets == 0) {
            // member is already done with the job and master didn't know it yet - we are immediately successful
            return completedFuture(new SnapshotOperationResult(0, 0, 0, null));
        }
        future = new CompletableFuture<>();
        return future;
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
    synchronized void taskletDone(long lastCompletedSnapshotId, boolean isHigherPrioritySource) {
        assert numTasklets > 0 : "numTasklets=" + numTasklets;
        assert lastCompletedSnapshotId <= activeSnapshotId : "activeSnapshotId=" + activeSnapshotId
                + ", tasklet.lastCompletedSnapshotId=" + lastCompletedSnapshotId;

        numTasklets--;
        if (isHigherPrioritySource) {
            assert numHigherPriorityTasklets > 0 : "numHigherPriorityTasklets=" + numHigherPriorityTasklets;
            numHigherPriorityTasklets--;
            // after all higher priority vertices are done we can start the snapshot
            if (numHigherPriorityTasklets == 0 && activeSnapshotId < currentSnapshotId) {
                activeSnapshotId = currentSnapshotId;
                logger.info("Postponed snapshot " + activeSnapshotId + " for " + jobNameAndExecutionId + " started");
            }
        }
        assert numHigherPriorityTasklets <= numTasklets : "numHigherPriorityTasklets > numTasklets";
        assert lastCompletedSnapshotId <= currentSnapshotId : "tasklet completed a snapshot that didn't start yet";

        if (lastCompletedSnapshotId < currentSnapshotId) {
            // if tasklet is done before it was aware of the current snapshot, we
            // treat it as if it already completed the snapshot without any data
            snapshotDoneForTasklet(0, 0, 0);
        }
    }

    /**
     * Called when current snapshot is done in {@link StoreSnapshotTasklet}
     * (it received barriers from all its processors and all async flush
     * operations are done).
     */
    void snapshotDoneForTasklet(long numBytes, long numKeys, long numChunks) {
        totalBytes.addAndGet(numBytes);
        totalKeys.addAndGet(numKeys);
        totalChunks.addAndGet(numChunks);
        int newRemainingTasklets = numRemainingTasklets.decrementAndGet();
        assert newRemainingTasklets >= 0 : "newRemainingTasklets=" + newRemainingTasklets;
        if (newRemainingTasklets == 0) {
            handleSnapshotDone();
        }
    }

    /**
     * Method is called when execution is cancelled/terminated. The existing
     * ongoing snapshot will be completed with failure and no more snapshots
     * will be allowed to start.
     */
    synchronized void cancel() {
        if (future != null) {
            reportError(new CancellationException("execution cancelled"));
            handleSnapshotDone();
        }
        isCancelled = true;
    }

    private synchronized void handleSnapshotDone() {
        if (isCancelled) {
            assert future == null : "future=" + future;
            return;
        }
        future.complete(
                new SnapshotOperationResult(totalBytes.get(), totalKeys.get(), totalChunks.get(), snapshotError.get()));

        future = null;
        snapshotError.set(null);
        totalBytes.set(0);
        totalKeys.set(0);
        totalChunks.set(0);
        currentMapName = null;
    }

    void reportError(Throwable ex) {
        snapshotError.compareAndSet(null, ex);
    }

    // public-visible for tests
    AtomicInteger getNumRemainingTasklets() {
        return numRemainingTasklets;
    }
}
