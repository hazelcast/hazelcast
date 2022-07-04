/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriter;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static com.hazelcast.jet.core.metrics.MetricNames.SNAPSHOT_BYTES;
import static com.hazelcast.jet.core.metrics.MetricNames.SNAPSHOT_KEYS;
import static com.hazelcast.jet.impl.execution.StoreSnapshotTasklet.State.DONE;
import static com.hazelcast.jet.impl.execution.StoreSnapshotTasklet.State.DRAIN;
import static com.hazelcast.jet.impl.execution.StoreSnapshotTasklet.State.FLUSH;
import static com.hazelcast.jet.impl.execution.StoreSnapshotTasklet.State.REACHED_BARRIER;

public class StoreSnapshotTasklet implements Tasklet {

    long pendingSnapshotId;

    private final SnapshotContext snapshotContext;
    private final InboundEdgeStream inboundEdgeStream;
    private final ILogger logger;
    private final String vertexName;
    private final boolean isHigherPrioritySource;

    private final AsyncSnapshotWriter ssWriter;
    private final ProgressTracker progTracker = new ProgressTracker();
    private final AtomicReference<LongLongAccumulator> metrics = new AtomicReference<>(new LongLongAccumulator());
    private State state = DRAIN;
    private boolean hasReachedBarrier;
    private Entry<Data, Data> pendingEntry;
    private Predicate<Object> addToInboxFunction;

    public StoreSnapshotTasklet(
            SnapshotContext snapshotContext,
            InboundEdgeStream inboundEdgeStream,
            AsyncSnapshotWriter ssWriter,
            ILogger logger,
            String vertexName,
            boolean isHigherPrioritySource
    ) {
        this.snapshotContext = snapshotContext;
        this.inboundEdgeStream = inboundEdgeStream;
        this.logger = logger;
        this.vertexName = vertexName;
        this.isHigherPrioritySource = isHigherPrioritySource;

        this.ssWriter = ssWriter;
        this.pendingSnapshotId = snapshotContext.activeSnapshotIdPhase1() + 1;
        addToInboxFunction = this::addToInbox;
    }

    @Nonnull @Override
    public ProgressState call() {
        progTracker.reset();
        stateMachineStep();
        return progTracker.toProgressState();
    }

    private void stateMachineStep() {
        switch (state) {
            case DRAIN:
                progTracker.notDone();
                if (pendingEntry != null) {
                    if (!ssWriter.offer(pendingEntry)) {
                        return;
                    }
                    progTracker.madeProgress();
                }
                pendingEntry = null;
                ProgressState result = inboundEdgeStream.drainTo(addToInboxFunction);
                if (result.isDone()) {
                    assert ssWriter.isEmpty() : "input is done, but we had some entries and not the barrier";
                    snapshotContext.storeSnapshotTaskletDone(pendingSnapshotId - 1, isHigherPrioritySource);
                    state = DONE;
                    progTracker.reset();
                }
                progTracker.madeProgress(result.isMadeProgress());
                if (hasReachedBarrier) {
                    state = FLUSH;
                    stateMachineStep();
                }
                break;

            case FLUSH:
                progTracker.notDone();
                if (ssWriter.flushAndResetMap()) {
                    progTracker.madeProgress();
                    state = REACHED_BARRIER;
                }
                break;

            case REACHED_BARRIER:
                if (ssWriter.hasPendingAsyncOps()) {
                    progTracker.notDone();
                    return;
                }
                // check for writing error
                Throwable error = ssWriter.getError();
                if (error != null) {
                    logger.severe("Error writing to snapshot map", error);
                    snapshotContext.reportError(error);
                }
                progTracker.madeProgress();
                long bytes = ssWriter.getTotalPayloadBytes();
                long keys = ssWriter.getTotalKeys();
                long chunks = ssWriter.getTotalChunks();
                snapshotContext.phase1DoneForTasklet(bytes, keys, chunks);
                metrics.set(new LongLongAccumulator(bytes, keys));
                ssWriter.resetStats();
                pendingSnapshotId++;
                hasReachedBarrier = false;
                state = DRAIN;
                progTracker.notDone();
                break;

            default:
                // note State.DONE also goes here
                throw new JetException("Unexpected state: " + state);
        }
    }

    private boolean addToInbox(Object o) {
        if (o instanceof SnapshotBarrier) {
            SnapshotBarrier barrier = (SnapshotBarrier) o;
            assert pendingSnapshotId == barrier.snapshotId() : "Unexpected barrier, expected was " +
                    pendingSnapshotId + ", but barrier was " + barrier.snapshotId() + ", this=" + this;
            hasReachedBarrier = true;
        } else {
            if (!ssWriter.offer((Entry<Data, Data>) o)) {
                pendingEntry = (Entry<Data, Data>) o;
                return false;
            }
        }
        return true;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        descriptor = descriptor.withTag(MetricTags.VERTEX, vertexName);

        LongLongAccumulator metricValues = metrics.get();
        context.collect(descriptor, SNAPSHOT_BYTES, ProbeLevel.INFO, ProbeUnit.COUNT, metricValues.get1());
        context.collect(descriptor, SNAPSHOT_KEYS, ProbeLevel.INFO, ProbeUnit.COUNT, metricValues.get2());
    }

    @Override
    public String toString() {
        return StoreSnapshotTasklet.class.getSimpleName() + '{' + vertexName + '}';
    }

    enum State {
        /** Draining the queue, flushing as necessary. */
        DRAIN,
        /** Wait until we are able to flush remaining buffers. */
        FLUSH,
        /** Wait for flushes to complete, then go to {@link #DRAIN} again. */
        REACHED_BARRIER,
        /** Input is done, terminal state. */
        DONE
    }
}
