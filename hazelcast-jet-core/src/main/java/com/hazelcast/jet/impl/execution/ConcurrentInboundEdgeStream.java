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
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.function.Predicate;

import java.util.BitSet;
import java.util.function.ToIntFunction;

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
    private final ConcurrentConveyor<Object> conveyor;
    private final ProgressTracker tracker = new ProgressTracker();
    private final ItemDetector itemDetector = new ItemDetector();

    private final WatermarkCoalescer watermarkCoalescer;
    private final BitSet receivedBarriers; // indicates if current snapshot is received on the queue
    private final ILogger logger;

    // Tells whether we are operating in exactly-once or at-least-once mode.
    // In other words, whether a barrier from all queues must be present before
    // draining more items from a queue where a barrier has been reached.
    // Once a terminal snapshot barrier is reached, this is always true.
    private boolean waitForAllBarriers;
    private SnapshotBarrier currentBarrier;  // next snapshot barrier to emit
    private long numActiveQueues; // number of active queues remaining

    /**
     * @param waitForAllBarriers If {@code true}, a queue that had a barrier won't
     *          be drained until the same barrier is received from all other
     *          queues. This will enforce exactly-once vs. at-least-once, if it
     *          is {@code false}.
     */
    public ConcurrentInboundEdgeStream(ConcurrentConveyor<Object> conveyor, int ordinal, int priority,
                                       boolean waitForAllBarriers, int maxWatermarkRetainMillis, String debugName) {
        this.conveyor = conveyor;
        this.ordinal = ordinal;
        this.priority = priority;
        this.waitForAllBarriers = waitForAllBarriers;

        watermarkCoalescer = WatermarkCoalescer.create(maxWatermarkRetainMillis, conveyor.queueCount());

        numActiveQueues = conveyor.queueCount();
        receivedBarriers = new BitSet(conveyor.queueCount());
        logger = Logger.getLogger(ConcurrentInboundEdgeStream.class.getName() + "." + debugName);
        logger.finest("Coalescing " + conveyor.queueCount() + " input queues");
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
    public ProgressState drainTo(Predicate<Object> dest) {
        return drainTo(watermarkCoalescer.getTime(), dest);
    }

    // package-visible for testing
    ProgressState drainTo(long now, Predicate<Object> dest) {
        tracker.reset();
        for (int queueIndex = 0; queueIndex < conveyor.queueCount(); queueIndex++) {
            final QueuedPipe<Object> q = conveyor.queue(queueIndex);
            if (q == null) {
                continue;
            }

            // skip queues where a snapshot barrier has already been received
            if (waitForAllBarriers && receivedBarriers.get(queueIndex)) {
                continue;
            }

            ProgressState result = drainQueue(q, dest);
            tracker.mergeWith(result);

            if (itemDetector.item == DONE_ITEM) {
                conveyor.removeQueue(queueIndex);
                receivedBarriers.clear(queueIndex);
                numActiveQueues--;
                long wmTimestamp = watermarkCoalescer.queueDone(queueIndex);
                if (maybeEmitWm(wmTimestamp, dest)) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("Queue " + queueIndex + " is done, forwarding " + new Watermark(wmTimestamp));
                    }
                    return numActiveQueues == 0 ? DONE : MADE_PROGRESS;
                }
            } else if (itemDetector.item instanceof Watermark) {
                long wmTimestamp = ((Watermark) itemDetector.item).timestamp();
                boolean forwarded = maybeEmitWm(watermarkCoalescer.observeWm(now, queueIndex, wmTimestamp), dest);
                if (logger.isFinestEnabled()) {
                    logger.finest("Received " + itemDetector.item + " from queue " + queueIndex
                            + (forwarded ? ", forwarded" : ", not forwarded"));
                }
                if (forwarded) {
                    return MADE_PROGRESS;
                }
            } else if (itemDetector.item instanceof SnapshotBarrier) {
                observeBarrier(queueIndex, (SnapshotBarrier) itemDetector.item);
            } else if (result.isMadeProgress()) {
                watermarkCoalescer.observeEvent(queueIndex);
            }

            if (numActiveQueues == 0) {
                return tracker.toProgressState();
            }

            if (itemDetector.item != null) {
                // if we have received the current snapshot from all active queues, forward it
                if (receivedBarriers.cardinality() == numActiveQueues) {
                    assert currentBarrier != null : "currentBarrier == null";
                    boolean res = dest.test(currentBarrier);
                    assert res : "test result expected to be true";
                    currentBarrier = null;
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

    private boolean maybeEmitWm(long timestamp, Predicate<Object> dest) {
        if (timestamp != NO_NEW_WM) {
            boolean res = dest.test(new Watermark(timestamp));
            assert res : "test result expected to be true";
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
    private ProgressState drainQueue(Pipe<Object> queue, Predicate<Object> dest) {
        itemDetector.reset(dest);

        int drainedCount = queue.drain(itemDetector);

        itemDetector.dest = null;
        return ProgressState.valueOf(drainedCount > 0, itemDetector.item == DONE_ITEM);
    }

    private void observeBarrier(int queueIndex, SnapshotBarrier barrier) {
        if (currentBarrier == null) {
            currentBarrier = barrier;
        } else {
            assert currentBarrier.equals(barrier) : currentBarrier + " != " + barrier;
        }
        if (barrier.isTerminal()) {
            // Switch to exactly-once mode. The reason is that there will be DONE_ITEM just after the
            // terminal barrier and if we process it before receiving the other barriers, it could cause
            // the watermark to advance. The exactly-once mode disallows processing of any items after
            // the barrier before the barrier is processed.
            waitForAllBarriers = true;
        }
        receivedBarriers.set(queueIndex);
    }

    /**
     * Drains a concurrent conveyor's queue while watching for {@link Watermark}s
     * and {@link SnapshotBarrier}s.
     * When encountering either of them it prevents draining more items.
     */
    private static final class ItemDetector implements Predicate<Object> {
        Predicate<Object> dest;
        BroadcastItem item;

        void reset(Predicate<Object> newDest) {
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
            return dest.test(o);
        }
    }

    @Override
    public int sizes() {
        return conveyorSum(QueuedPipe::size);
    }

    @Override
    public int capacities() {
        return conveyorSum(QueuedPipe::capacity);
    }

    private int conveyorSum(ToIntFunction<QueuedPipe<Object>> toIntF) {
        int sum = 0;
        for (int queueIndex = 0; queueIndex < conveyor.queueCount(); queueIndex++) {
            final QueuedPipe<Object> q = conveyor.queue(queueIndex);
            if (q != null) {
                sum += toIntF.applyAsInt(q);
            }
        }
        return sum;
    }
}
