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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.SnapshotRepository;
import com.hazelcast.jet.impl.util.AsyncMapWriter;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.impl.execution.StoreSnapshotTasklet.State.DONE;
import static com.hazelcast.jet.impl.execution.StoreSnapshotTasklet.State.DRAIN;
import static com.hazelcast.jet.impl.execution.StoreSnapshotTasklet.State.FLUSH;
import static com.hazelcast.jet.impl.execution.StoreSnapshotTasklet.State.REACHED_BARRIER;

public class StoreSnapshotTasklet implements Tasklet {

    private final ProgressTracker progTracker = new ProgressTracker();
    private final long jobId;
    private final InboundEdgeStream inboundEdgeStream;
    private final SnapshotContext snapshotContext;
    private final AsyncMapWriter mapWriter;
    private final boolean isHigherPrioritySource;
    private final String vertexName;

    private long pendingSnapshotId;

    private AtomicInteger numActiveFlushes = new AtomicInteger();

    private State state = DRAIN;
    private boolean hasReachedBarrier;
    private boolean inputIsDone;

    public StoreSnapshotTasklet(SnapshotContext snapshotContext, long jobId, InboundEdgeStream inboundEdgeStream,
                                NodeEngine nodeEngine, String vertexName, boolean isHigherPrioritySource) {
        this.snapshotContext = snapshotContext;
        this.jobId = jobId;
        this.inboundEdgeStream = inboundEdgeStream;
        this.vertexName = vertexName;
        this.isHigherPrioritySource = isHigherPrioritySource;

        this.mapWriter = new AsyncMapWriter(nodeEngine);
        this.mapWriter.setMapName(currMapName());
        this.pendingSnapshotId = snapshotContext.lastSnapshotId() + 1;
    }

    @Nonnull
    @Override
    public ProgressState call() {
        progTracker.reset();
        stateMachineStep();
        return progTracker.toProgressState();
    }

    private void stateMachineStep() {
        switch (state) {
            case DRAIN:
                progTracker.notDone();
                ProgressState result = inboundEdgeStream.drainTo(o -> {
                    if (o instanceof SnapshotBarrier) {
                        SnapshotBarrier barrier = (SnapshotBarrier) o;
                        assert pendingSnapshotId == barrier.snapshotId() : "Unexpected barrier, expected was " +
                                pendingSnapshotId + ", but barrier was " + barrier.snapshotId() + ", this=" + this;
                        hasReachedBarrier = true;
                    } else {
                        mapWriter.put(((Entry<Data, Data>) o));
                    }
                });
                if (result.isDone()) {
                    inputIsDone = true;
                }
                if (result.isMadeProgress()) {
                    progTracker.madeProgress();
                    state = FLUSH;
                    stateMachineStep();
                }
                return;

            case FLUSH:
                progTracker.notDone();
                CompletableFuture<Void> future = new CompletableFuture<>();
                future.whenComplete((r, t) -> {
                    if (t == null) {
                        numActiveFlushes.decrementAndGet();
                    } else {
                        //TODO: error handling
                        t.printStackTrace();
                    }
                });
                if (mapWriter.tryFlushAsync(future)) {
                    progTracker.madeProgress();
                    numActiveFlushes.incrementAndGet();
                    state = inputIsDone ? DONE : hasReachedBarrier ? REACHED_BARRIER : DRAIN;
                }
                return;

            case REACHED_BARRIER:
                progTracker.notDone();
                if (numActiveFlushes.get() == 0) {
                    snapshotContext.snapshotDoneForTasklet();
                    pendingSnapshotId++;
                    mapWriter.setMapName(currMapName());
                    state = inputIsDone ? DONE : DRAIN;
                    hasReachedBarrier = false;
                }
                return;

            case DONE:
                if (numActiveFlushes.get() != 0) {
                    progTracker.notDone();
                }
                snapshotContext.taskletDone(pendingSnapshotId - 1, isHigherPrioritySource);
                return;

            default:
                throw new JetException("Unexpected state: " + state);
        }
    }

    private String currMapName() {
        return SnapshotRepository.snapshotDataMapName(jobId, pendingSnapshotId, vertexName);
    }

    @Override
    public String toString() {
        return StoreSnapshotTasklet.class.getSimpleName() + ", vertex:" + vertexName;
    }

    enum State {
        DRAIN,
        FLUSH,
        REACHED_BARRIER,
        DONE
    }
}
