/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

    public static final int NO_SNAPSHOT = -1;

    private final ILogger logger;

    private final String jobNameAndExecutionId;
    private final ProcessingGuarantee guarantee;

    /**
     * SnapshotId of last snapshot created. Source processors read
     * it and when they see changed value, they start a snapshot with that
     * ID. {@code -1} means no snapshot was started.
     */
    private final AtomicLong lastSnapshotId;

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
     * Remaining number of {@link StoreSnapshotTasklet}s in currently produced
     * snapshot. When it is decremented to 0, the snapshot is complete and new
     * one can start.
     * <p>
     * It can have negative value in case described in {@link #startNewSnapshot}.
     */
    private final AtomicInteger numRemainingTasklets = new AtomicInteger();

    /**
     * Holder for the snapshot error, if any
     */
    private final AtomicReference<Throwable> snapshotError = new AtomicReference<>();

    /**
     * True, if a snapshot was started and postponed. If true, then when
     * {@link #numHigherPriorityTasklets} is decremented to 0, snapshot is
     * started.
     */
    private boolean snapshotPostponed;

    /**
     * Future which will be created when a snapshot starts and completed and
     * nulled out when the snapshot completes.
     */
    private volatile CompletableFuture<SnapshotOperationResult> future;

    private final AtomicLong totalBytes = new AtomicLong();
    private final AtomicLong totalKeys = new AtomicLong();
    private final AtomicLong totalChunks = new AtomicLong();
    private boolean isCancelled;

    SnapshotContext(ILogger logger, String jobNameAndExecutionId, long lastSnapshotId,
                    ProcessingGuarantee guarantee
    ) {
        this.jobNameAndExecutionId = jobNameAndExecutionId;
        this.lastSnapshotId = new AtomicLong(lastSnapshotId);
        this.guarantee = guarantee;
        this.logger = logger;
    }

    /**
     * Id of the last started snapshot
     */
    long lastSnapshotId() {
        return lastSnapshotId.get();
    }

    boolean isTerminalSnapshot() {
        return isTerminal;
    }

    ProcessingGuarantee processingGuarantee() {
        return guarantee;
    }

    synchronized void initTaskletCount(int taskletCount, int highPriorityTaskletCount) {
        assert this.numTasklets == Integer.MIN_VALUE : "Tasklet count already set once.";
        assert taskletCount >= highPriorityTaskletCount :
                "taskletCount=" + taskletCount + ", highPriorityTaskletCount=" + highPriorityTaskletCount;
        assert taskletCount > 0 : "taskletCount=" + taskletCount;
        assert highPriorityTaskletCount >= 0 : "highPriorityTaskletCount=" + highPriorityTaskletCount;

        this.numTasklets = taskletCount;
        this.numHigherPriorityTasklets = highPriorityTaskletCount;
    }

    /**
     * This method is called when the member received {@link
     * com.hazelcast.jet.impl.operation.SnapshotOperation}.
     * <p>
     * <b>Note:</b> this method can be called <i>after</i> {@link
     * #taskletDone(long, boolean)} or {@link #snapshotDoneForTasklet} is
     * called. This can happen in a situation when a processor only has input
     * queues from remote members and the remote members happen to process
     * {@code SnapshotOperation} and send barriers to such processor before
     * the {@code SnapshotOperation} is called on this member.
     */
    synchronized CompletableFuture<SnapshotOperationResult> startNewSnapshot(long snapshotId, boolean isTerminal) {
        assert snapshotId == lastSnapshotId.get() + 1
                : "new snapshotId not incremented by 1. Previous=" + lastSnapshotId + ", new=" + snapshotId;
        assert numTasklets >= 0 : "numTasklets=" + numTasklets;
        if (isCancelled) {
            throw new CancellationException("execution cancelled");
        }
        this.isTerminal = isTerminal;
        int newNumRemainingTasklets = numRemainingTasklets.addAndGet(numTasklets);
        assert newNumRemainingTasklets - numTasklets <= 0 :
                "previous snapshot was not finished, numRemainingTasklets=" + (newNumRemainingTasklets - numTasklets);
        // if there are no higher priority tasklets, start the snapshot now
        if (numHigherPriorityTasklets == 0) {
            lastSnapshotId.set(snapshotId);
        } else {
            logger.warning("Snapshot " + snapshotId + " for " + jobNameAndExecutionId + " is postponed" +
                    " until all higher priority vertices are completed (number of such vertices = "
                    + numHigherPriorityTasklets + ')');
            snapshotPostponed = true;
        }
        if (numTasklets == 0) {
            // member is already done with the job and master didn't know it yet - we are immediately successful
            return completedFuture(new SnapshotOperationResult(0, 0, 0, null));
        }
        CompletableFuture<SnapshotOperationResult> res = future = new CompletableFuture<>();
        if (newNumRemainingTasklets == 0) {
            handleSnapshotDone();
        }
        return res;
    }

    /**
     * Called when {@link StoreSnapshotTasklet} is done (received DONE_ITEM
     * from all its processors).
     *
     * @param lastSnapshotId id of the last snapshot completed by the tasklet
     */
    synchronized void taskletDone(long lastSnapshotId, boolean isHigherPrioritySource) {
        assert numTasklets > 0 : "numTasklets=" + numTasklets;
        assert lastSnapshotId <= this.lastSnapshotId.get() + 1 : "this.lastSnapshotId=" + this.lastSnapshotId.get()
                + "tasklet.lastSnapshotId=" + lastSnapshotId;

        numTasklets--;
        if (isHigherPrioritySource) {
            assert numHigherPriorityTasklets > 0 : "numHigherPriorityTasklets=" + numHigherPriorityTasklets;
            numHigherPriorityTasklets--;
            // after all higher priority vertices are done we can start the snapshot
            if (numHigherPriorityTasklets == 0 && snapshotPostponed) {
                this.lastSnapshotId.incrementAndGet();
                logger.info("Postponed snapshot " + this.lastSnapshotId + " for " + jobNameAndExecutionId
                        + " started");
            }
        }
        if (this.lastSnapshotId.get() > lastSnapshotId) {
            snapshotDoneForTasklet(0, 0, 0);
        } else if (this.lastSnapshotId.get() < lastSnapshotId) {
            // tasklet is done with snapshot before startNewSnapshot was called
            numRemainingTasklets.incrementAndGet();
        }
    }

    /**
     * Called when current snapshot is done in {@link StoreSnapshotTasklet}
     * (it received barriers from all its processors and all async flush
     * operations are done).
     * <p>
     * This method can be called before the snapshot was started with {@link
     * #startNewSnapshot}. This can happen, if the processor only has input
     * queues from remote members, from which it can possibly receive barriers
     * before {@link SnapshotOperation} is handled on this member.
     */
    void snapshotDoneForTasklet(long numBytes, long numKeys, long numChunks) {
        totalBytes.addAndGet(numBytes);
        totalKeys.addAndGet(numKeys);
        totalChunks.addAndGet(numChunks);
        // note that numRemainingTasklets can get negative values here.
        if (numRemainingTasklets.decrementAndGet() == 0) {
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
    }

    void reportError(Throwable ex) {
        snapshotError.compareAndSet(null, ex);
    }

    // public-visible for tests
    public AtomicInteger getNumRemainingTasklets() {
        return numRemainingTasklets;
    }
}
