/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.ILogger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.impl.util.Util.completeVoidFuture;
import static com.hazelcast.jet.impl.util.Util.completedVoidFuture;
import static com.hazelcast.jet.impl.util.Util.jobAndExecutionId;

public class SnapshotContext {

    public static final int NO_SNAPSHOT = -1;

    /**
     * Remaining number of {@link StoreSnapshotTasklet}s in currently produced
     * snapshot. When it is decremented to 0, the snapshot is complete and new
     * one can start.
     */
    // package-visible for test
    final AtomicInteger numRemainingTasklets = new AtomicInteger();

    private final ILogger logger;

    private final long jobId;
    private final long executionId;
    private final ProcessingGuarantee guarantee;

    /**
     * SnapshotId of last snapshot created. Source processors read
     * it and when they see changed value, they start a snapshot with that
     * ID. {@code -1} means no snapshot was started.
     */
    private final AtomicLong lastSnapshotId;

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
     * Holder for the snapshot error, if any
     */
    private final AtomicReference<Throwable> snapshotError = new AtomicReference<>();

    /**
     * True, if a snapshot was started and postponed. If true, then when
     * {@link #numHigherPriorityTasklets} is decremented to 0, snapshot is
     * started.
     */
    private boolean snapshotPostponed;

    /** Future which will be completed when the current snapshot completes. */
    private volatile CompletableFuture<Void> future;

    SnapshotContext(ILogger logger, long jobId, long executionId, long lastSnapshotId,
                    ProcessingGuarantee guarantee
    ) {
        this.jobId = jobId;
        this.executionId = executionId;
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

    ProcessingGuarantee processingGuarantee() {
        return guarantee;
    }

    synchronized void initTaskletCount(int totalCount, int highPriorityCount) {
        assert this.numTasklets == Integer.MIN_VALUE : "Tasklet count already set once.";
        assert totalCount >= highPriorityCount : "totalCount=" + totalCount + ", highPriorityCount=" + highPriorityCount;
        assert totalCount > 0 : "totalCount=" + totalCount;
        assert highPriorityCount >= 0 : "highPriorityCount=" + highPriorityCount;

        this.numTasklets = totalCount;
        this.numHigherPriorityTasklets = highPriorityCount;
    }

    /**
     * This method is called when the member received {@link
     * com.hazelcast.jet.impl.operation.SnapshotOperation}.
     */
    synchronized CompletableFuture<Void> startNewSnapshot(long snapshotId) {
        assert snapshotId == lastSnapshotId.get() + 1
                : "new snapshotId not incremented by 1. Previous=" + lastSnapshotId + ", new=" + snapshotId;
        assert numTasklets >= 0 : "numTasklets=" + numTasklets;

        if (numTasklets == 0) {
            // member is already done with the job and master didn't know it yet - we are immediately done.
            return completedVoidFuture();
        }
        boolean success = numRemainingTasklets.compareAndSet(0, numTasklets);
        assert success : "previous snapshot was not finished, numRemainingTasklets=" + numRemainingTasklets.get();
        // if there are no higher priority tasklets, start the snapshot now
        if (numHigherPriorityTasklets == 0) {
            lastSnapshotId.set(snapshotId);
        } else {
            logger.warning("Snapshot " + snapshotId + " for " + jobAndExecutionId(jobId, executionId) + " is postponed" +
                    " until all higher priority vertices are completed (number of vertices = "
                    + numHigherPriorityTasklets + ')');
            snapshotPostponed = true;
        }
        return (future = new CompletableFuture<>());
    }

    /**
     * Called when {@link StoreSnapshotTasklet} is done (received DONE_ITEM
     * from all its processors).
     *
     * @param lastSnapshotId id fo the last snapshot completed by the tasklet
     */
    synchronized void taskletDone(long lastSnapshotId, boolean isHigherPrioritySource) {
        assert numTasklets > 0;
        assert lastSnapshotId <= this.lastSnapshotId.get();

        numTasklets--;
        if (isHigherPrioritySource) {
            assert numHigherPriorityTasklets > 0;
            numHigherPriorityTasklets--;
            // after all higher priority vertices are done we can start the snapshot
            if (numHigherPriorityTasklets == 0 && snapshotPostponed) {
                this.lastSnapshotId.incrementAndGet();
                logger.info("Postponed snapshot " + this.lastSnapshotId + " for " + jobAndExecutionId(jobId, executionId)
                        + " started");
            }
        }
        if (this.lastSnapshotId.get() > lastSnapshotId) {
            snapshotDoneForTasklet();
        }
    }

    /**
     * Called when current snapshot is done in {@link StoreSnapshotTasklet}
     * (it received barriers from all its processors and all async flush
     * operations are done).
     */
    void snapshotDoneForTasklet() {
        int oldValue = numRemainingTasklets.get();
        assert oldValue > 0 : "oldValue=" + oldValue;

        if (numRemainingTasklets.decrementAndGet() == 0) {
            Throwable t = snapshotError.get();
            if (t == null) {
                completeVoidFuture(future);
            } else {
                future.completeExceptionally(t);
            }
            future = null;
            snapshotError.set(null);
        }
    }

    void reportError(Throwable ex) {
        snapshotError.compareAndSet(null, ex);
    }
}
