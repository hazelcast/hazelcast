/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.ComparatorEx;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.Pipe;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.util.ObjectWithPartitionId;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.Comparator;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.NO_NEW_WM;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.Util.toLocalTime;

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
    private Comparator<Object> comparator;
    private final WatermarkCoalescer watermarkCoalescer;
    private final BitSet receivedBarriers; // indicates if current snapshot is received on the queue
    private final ILogger logger;

    // Tells whether we are operating in exactly-once or at-least-once mode.
    // In other words, whether a barrier from all queues must be present before
    // draining more items from a queue where a barrier has been reached.
    // Once a terminal snapshot barrier is reached, this is always true.
    private boolean waitForAllBarriers;
    private SnapshotBarrier currentBarrier;  // next snapshot barrier to emit

    /**
     * @param waitForAllBarriers If {@code true}, a queue that had a barrier won't
     *          be drained until the same barrier is received from all other
     *          queues. This will enforce exactly-once vs. at-least-once, if it
     *          is {@code false}.
     */
    public ConcurrentInboundEdgeStream(
            @Nonnull ConcurrentConveyor<Object> conveyor, int ordinal, int priority, boolean waitForAllBarriers,
            @Nonnull String debugName
    ) {
        this.conveyor = conveyor;
        this.ordinal = ordinal;
        this.priority = priority;
        this.waitForAllBarriers = waitForAllBarriers;

        watermarkCoalescer = WatermarkCoalescer.create(conveyor.queueCount());
        receivedBarriers = new BitSet(conveyor.queueCount());
        logger = Logger.getLogger(ConcurrentInboundEdgeStream.class.getName() + "." + debugName);
        logger.finest("Coalescing " + conveyor.queueCount() + " input queues");
    }

    @SuppressWarnings("unchecked")
    public ConcurrentInboundEdgeStream(
            @Nonnull ConcurrentConveyor<Object> conveyor, int ordinal, int priority, boolean waitForAllBarriers,
            @Nonnull String debugName, @Nonnull ComparatorEx<?> comparator
    ) {
        this(conveyor, ordinal, priority, waitForAllBarriers, debugName);
        this.comparator = (Comparator<Object>) comparator;
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    @Override
    public int priority() {
        return priority;
    }

    @Nonnull @Override
    public ProgressState drainTo(@Nonnull Predicate<Object> dest) {
        if (comparator != null) {
            return drainToWithComparator(dest);
        }
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
                long wmTimestamp = watermarkCoalescer.queueDone(queueIndex);
                if (maybeEmitWm(wmTimestamp, dest)) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("Queue " + queueIndex + " is done, forwarding " + new Watermark(wmTimestamp));
                    }
                    return conveyor.liveQueueCount() == 0 ? DONE : MADE_PROGRESS;
                }
            } else if (itemDetector.item instanceof Watermark) {
                long wmTimestamp = ((Watermark) itemDetector.item).timestamp();
                boolean forwarded = maybeEmitWm(watermarkCoalescer.observeWm(queueIndex, wmTimestamp), dest);
                if (logger.isFinestEnabled()) {
                    logger.finest("Received " + itemDetector.item + " from queue " + queueIndex
                            + (forwarded ? ", forwarded=" : ", not forwarded")
                            + ", coalescedWm=" + toLocalTime(watermarkCoalescer.coalescedWm())
                            + ", topObservedWm=" + toLocalTime(topObservedWm()));
                }
                if (forwarded) {
                    return MADE_PROGRESS;
                }
            } else if (itemDetector.item instanceof SnapshotBarrier) {
                observeBarrier(queueIndex, (SnapshotBarrier) itemDetector.item);
            } else if (result.isMadeProgress()) {
                watermarkCoalescer.observeEvent(queueIndex);
            }

            int liveQueueCount = conveyor.liveQueueCount();
            if (liveQueueCount == 0) {
                return tracker.toProgressState();
            }
            // if we have received the current snapshot from all active queues, forward it
            if (itemDetector.item != null && receivedBarriers.cardinality() == liveQueueCount) {
                assert currentBarrier != null : "currentBarrier == null";
                boolean res = dest.test(currentBarrier);
                assert res : "test result expected to be true";
                currentBarrier = null;
                receivedBarriers.clear();
                return MADE_PROGRESS;
            }
        }

        // try to emit WM based on history
        if (maybeEmitWm(watermarkCoalescer.checkWmHistory(), dest)) {
            return MADE_PROGRESS;
        }

        if (conveyor.liveQueueCount() > 0) {
            tracker.notDone();
        }
        return tracker.toProgressState();
    }

    private ProgressState drainToWithComparator(Predicate<Object> dest) {
        tracker.reset();
        tracker.notDone();
        int batchSize = -1;
        Object lastItem = null;
        do {
            int minIndex = 0;
            Object minItem = null;
            for (int queueIndex = 0; queueIndex < conveyor.queueCount(); queueIndex++) {
                final QueuedPipe<Object> q = conveyor.queue(queueIndex);
                if (q == null) {
                    continue;
                }
                Object headItem = unwrap(q.peek());
                if (headItem == null) {
                    return tracker.toProgressState();
                }
                if (headItem == DONE_ITEM) {
                    conveyor.removeQueue(queueIndex);
                    continue;
                }
                if (minItem == null || comparator.compare(minItem, headItem) > 0) {
                    minIndex = queueIndex;
                    minItem = headItem;
                }
            }
            if (conveyor.liveQueueCount() == 0) {
                tracker.done();
                return tracker.toProgressState();
            }
            if (batchSize == -1) {
                batchSize = conveyor.queue(minIndex).size();
            }
            if (lastItem != null && comparator.compare(lastItem, minItem) > 0) {
                throw new JetException(String.format(
                    "Disorder on a monotonicOrder edge: received %s and then %s from the same queue",
                        lastItem, minItem
                ));
            }
            lastItem = minItem;
            Object polledItem = unwrap(conveyor.queue(minIndex).poll());
            tracker.madeProgress();
            assert polledItem == minItem : "polledItem != minItem";
            boolean consumeResult = dest.test(minItem);
            assert consumeResult : "consumeResult is false";
        } while (--batchSize > 0);
        return tracker.toProgressState();
    }

    private Object unwrap(Object item) {
        return item instanceof ObjectWithPartitionId
                ? ((ObjectWithPartitionId) item).getItem()
                : item;
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
        return conveyor.liveQueueCount() == 0;
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

    @Override
    public long topObservedWm() {
        return watermarkCoalescer.topObservedWm();
    }

    @Override
    public long coalescedWm() {
        return watermarkCoalescer.coalescedWm();
    }
}
