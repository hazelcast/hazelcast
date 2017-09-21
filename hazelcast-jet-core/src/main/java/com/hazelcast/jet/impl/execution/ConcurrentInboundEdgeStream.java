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

import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.Pipe;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.util.function.Predicate;

import java.util.Arrays;
import java.util.BitSet;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;

/**
 * {@link InboundEdgeStream} implemented in terms of a {@link ConcurrentConveyor}.
 * The conveyor has as many 1-to-1 concurrent queues as there are upstream tasklets
 * contributing to it.
 */
public class ConcurrentInboundEdgeStream implements InboundEdgeStream {

    private final int ordinal;
    private final int priority;
    private final ConcurrentConveyor<Object> conveyor;
    private final ProgressTracker tracker = new ProgressTracker();
    private final ItemDetector itemDetector = new ItemDetector();
    private final boolean waitForSnapshot;
    private final long[] queueWms;

    private final BitSet receivedBarriers; // indicates if current snapshot is received on the queue

    private long pendingSnapshotId; // next snapshot barrier to emit
    private long lastEmittedWm = Long.MIN_VALUE;

    private long numActiveQueues; // number of active queues remaining

    /**
     * @param waitForSnapshot If true, queues won't be drained until the same
     *                        barrier is received from all of them. This will enforce exactly once
     *                        vs. at least once, if it is false.
     */
    public ConcurrentInboundEdgeStream(ConcurrentConveyor<Object> conveyor, int ordinal, int priority,
                                       long lastSnapshotId, boolean waitForSnapshot) {
        this.conveyor = conveyor;
        this.ordinal = ordinal;
        this.priority = priority;
        this.waitForSnapshot = waitForSnapshot;

        queueWms = new long[conveyor.queueCount()];
        Arrays.fill(queueWms, Long.MIN_VALUE);

        numActiveQueues = conveyor.queueCount();
        receivedBarriers = new BitSet(conveyor.queueCount());
        pendingSnapshotId = lastSnapshotId + 1;
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    @Override
    public int priority() {
        return priority;
    }

    @Override
    public ProgressState drainTo(Consumer<Object> dest) {
        tracker.reset();
        for (int queueIndex = 0; queueIndex < conveyor.queueCount(); queueIndex++) {
            final QueuedPipe<Object> q = conveyor.queue(queueIndex);
            if (q == null) {
                continue;
            }

            // skip queues where a snapshot barrier has already been received
            if (waitForSnapshot && receivedBarriers.get(queueIndex)) {
                continue;
            }

            drainQueue(q, dest);

            if (itemDetector.isDone) {
                conveyor.removeQueue(queueIndex);
                receivedBarriers.clear(queueIndex);
                numActiveQueues--;
            } else if (itemDetector.wm != null) {
                observeWm(queueIndex, itemDetector.wm.timestamp());
            } else if (itemDetector.barrier != null) {
                observeBarrier(queueIndex, itemDetector.barrier.snapshotId());
            }
        }

        if (numActiveQueues == 0) {
            return tracker.toProgressState();
        }

        tracker.notDone();

        // coalesce WMs received and emit new WM if needed
        long bottomWm = bottomObservedWm();
        if (bottomWm > lastEmittedWm) {
            lastEmittedWm = bottomWm;
            dest.accept(new Watermark(bottomWm));
        }

        // if we have received the current snapshot from all active queues, forward it
        if (receivedBarriers.cardinality() == numActiveQueues) {
            dest.accept(new SnapshotBarrier(pendingSnapshotId));
            pendingSnapshotId++;
            receivedBarriers.clear();
        }
        return tracker.toProgressState();
    }

    @Override
    public boolean isDone() {
        return numActiveQueues == 0;
    }

    /**
     * Drains the supplied queue into a {@code dest} collection, up to the next
     * {@link Watermark} or {@link SnapshotBarrier}. Also updates the {@code tracker} with new status.
     *
     */
    private void drainQueue(Pipe<Object> queue, Consumer<Object> dest) {
        itemDetector.reset(dest);

        int drainedCount = queue.drain(itemDetector);
        tracker.mergeWith(ProgressState.valueOf(drainedCount > 0, itemDetector.isDone));

        itemDetector.dest = null;
    }

    private void observeBarrier(int queueIndex, long snapshotId) {
        // TODO basri is this necessary? can't we just check monotonicity?
        if (snapshotId != pendingSnapshotId) {
            throw new JetException("Unexpected snapshot barrier "
                    + snapshotId + ", expected " + pendingSnapshotId);
        }
        receivedBarriers.set(queueIndex);
    }


    private void observeWm(int queueIndex, final long wmValue) {
        if (queueWms[queueIndex] >= wmValue) {
            throw new JetException("Watermarks not monotonically increasing on queue: " +
                    "last one=" + queueWms[queueIndex] + ", new one=" + wmValue);
        }
        queueWms[queueIndex] = wmValue;
    }

    private long bottomObservedWm() {
        long min = queueWms[0];
        for (int i = 1; i < queueWms.length; i++) {
            if (queueWms[i] < min) {
                min = queueWms[i];
            }
        }
        return min;
    }

    /**
     * Drains a concurrent conveyor's queue while watching for {@link Watermark}s
     * and {@link SnapshotBarrier}s.
     * When encountering a either, prevents draining more items.
     */
    private static final class ItemDetector implements Predicate<Object> {
        Consumer<Object> dest;
        Watermark wm;
        SnapshotBarrier barrier;
        boolean isDone;

        void reset(Consumer<Object> newDest) {
            dest = newDest;
            wm = null;
            isDone = false;
            barrier = null;
        }

        @Override
        public boolean test(Object o) {
            if (o instanceof Watermark) {
                assert wm == null : "Received multiple Watermarks without a call to reset()";
                wm = (Watermark) o;
                return false;
            }
            if (o instanceof SnapshotBarrier) {
                assert barrier == null : "Received multiple barriers without a call to reset()";
                barrier = (SnapshotBarrier) o;
                return false;
            }
            if (o == DONE_ITEM) {
                isDone = true;
                return false;
            }
            dest.accept(o);
            return true;
        }
    }
}
