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

import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.Pipe;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.function.Predicate;

import java.util.BitSet;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.NO_NEW_WM;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;

/**
 * {@link InboundEdgeStream} implemented in terms of a {@link ConcurrentConveyor}.
 * The conveyor has as many 1-to-1 concurrent queues as there are upstream tasklets
 * contributing to it.
 */
public class ConcurrentInboundEdgeStream implements InboundEdgeStream {

    private final int ordinal;
    private final int priority;
    private final boolean waitForSnapshot;
    private final ConcurrentConveyor<Object> conveyor;
    private final ProgressTracker tracker = new ProgressTracker();
    private final ItemDetector itemDetector = new ItemDetector();

    private final WatermarkCoalescer watermarkCoalescer;
    private final BitSet receivedBarriers; // indicates if current snapshot is received on the queue
    private final ILogger logger;
    private long pendingSnapshotId; // next snapshot barrier to emit
    private long numActiveQueues; // number of active queues remaining

    /**
     * @param waitForSnapshot If {@code true}, a queue that had a barrier won't
     *          be drained until the same barrier is received from all other
     *          queues. This will enforce exactly-once vs. at-least-once, if it
     *          is {@code false}.
     */
    public ConcurrentInboundEdgeStream(ConcurrentConveyor<Object> conveyor, int ordinal, int priority,
                                       long lastSnapshotId, boolean waitForSnapshot, int maxWatermarkRetainMillis,
                                       String debugName) {
        this.conveyor = conveyor;
        this.ordinal = ordinal;
        this.priority = priority;
        this.waitForSnapshot = waitForSnapshot;

        watermarkCoalescer = WatermarkCoalescer.create(maxWatermarkRetainMillis, conveyor.queueCount());

        numActiveQueues = conveyor.queueCount();
        receivedBarriers = new BitSet(conveyor.queueCount());
        pendingSnapshotId = lastSnapshotId + 1;
        logger = Logger.getLogger(ConcurrentInboundEdgeStream.class.getName() + "." + debugName);
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
        return drainTo(watermarkCoalescer.getTime(), dest);
    }

    // package-visible for testing
    ProgressState drainTo(long now, Consumer<Object> dest) {
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

            ProgressState result = drainQueue(q, dest);
            tracker.mergeWith(result);

            if (itemDetector.item == DONE_ITEM) {
                conveyor.removeQueue(queueIndex);
                receivedBarriers.clear(queueIndex);
                numActiveQueues--;
                if (maybeEmitWm(watermarkCoalescer.queueDone(queueIndex), dest)) {
                    return numActiveQueues == 0 ? DONE : MADE_PROGRESS;
                }
            } else if (itemDetector.item instanceof Watermark) {
                long wmTimestamp = ((Watermark) itemDetector.item).timestamp();
                boolean forwarded = maybeEmitWm(watermarkCoalescer.observeWm(now, queueIndex, wmTimestamp), dest);
                if (logger.isFinestEnabled()) {
                    logger.finest("Received " + itemDetector.item + " from queue " + queueIndex + '/'
                            + conveyor.queueCount() + (forwarded ? ", forwarded" : ", not forwarded"));
                }
                if (forwarded) {
                    return MADE_PROGRESS;
                }
            } else if (itemDetector.item instanceof SnapshotBarrier) {
                observeBarrier(queueIndex, ((SnapshotBarrier) itemDetector.item).snapshotId());
            } else if (result.isMadeProgress()) {
                watermarkCoalescer.observeEvent(queueIndex);
            }

            if (numActiveQueues == 0) {
                return tracker.toProgressState();
            }

            if (itemDetector.item != null) {
                // if we have received the current snapshot from all active queues, forward it
                if (receivedBarriers.cardinality() == numActiveQueues) {
                    dest.accept(new SnapshotBarrier(pendingSnapshotId));
                    pendingSnapshotId++;
                    receivedBarriers.clear();
                    return MADE_PROGRESS;
                }
            }
        }

        // try to emit WM based on history
        if (maybeEmitWm(watermarkCoalescer.checkWmHistory(now), dest)) {
            return MADE_PROGRESS;
        }

        if (numActiveQueues > 0) {
            tracker.notDone();
        }
        return tracker.toProgressState();
    }

    private boolean maybeEmitWm(long timestamp, Consumer<Object> dest) {
        if (timestamp != NO_NEW_WM) {
            dest.accept(new Watermark(timestamp));
            return true;
        }
        return false;
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
    private ProgressState drainQueue(Pipe<Object> queue, Consumer<Object> dest) {
        itemDetector.reset(dest);

        int drainedCount = queue.drain(itemDetector);

        itemDetector.dest = null;
        return ProgressState.valueOf(drainedCount > 0, itemDetector.item == DONE_ITEM);
    }

    private void observeBarrier(int queueIndex, long snapshotId) {
        if (snapshotId != pendingSnapshotId) {
            throw new JetException("Unexpected snapshot barrier "
                    + snapshotId + ", expected " + pendingSnapshotId);
        }
        receivedBarriers.set(queueIndex);
    }

    /**
     * Drains a concurrent conveyor's queue while watching for {@link Watermark}s
     * and {@link SnapshotBarrier}s.
     * When encountering either of them it prevents draining more items.
     */
    private static final class ItemDetector implements Predicate<Object> {
        Consumer<Object> dest;
        BroadcastItem item;

        void reset(Consumer<Object> newDest) {
            dest = newDest;
            item = null;
        }

        @Override
        public boolean test(Object o) {
            if (o instanceof Watermark || o instanceof SnapshotBarrier || o == DONE_ITEM) {
                assert item == null : "Received multiple special items without a call to reset(): " + item;
                item = (BroadcastItem) o;
                return false;
            }
            dest.accept(o);
            return true;
        }
    }
}
