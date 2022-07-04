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

import com.hazelcast.function.ComparatorEx;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.Pipe;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.NO_NEW_WM;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefixedLogger;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.Util.toLocalTime;

/**
 * This non-instantiable class contains implementations of {@link
 * InboundEdgeStream} in terms of a {@link ConcurrentConveyor}. The
 * conveyor has as many 1-to-1 concurrent queues as there are upstream
 * tasklets contributing to it.
 */
public final class ConcurrentInboundEdgeStream {

    // Prevents instantiation
    private ConcurrentInboundEdgeStream() {
    }

    /**
     * @param waitForAllBarriers If {@code true}, a queue that had a barrier won't
     *          be drained until the same barrier is received from all other
     *          queues. This will enforce exactly-once vs. at-least-once, if it
     *          is {@code false}.
     */
    public static InboundEdgeStream create(
            @Nonnull ConcurrentConveyor<Object> conveyor,
            int ordinal,
            int priority,
            boolean waitForAllBarriers,
            @Nonnull String debugName,
            @Nullable ComparatorEx<?> comparator
    ) {
        if (comparator == null) {
            return new RoundRobinDrain(conveyor, ordinal, priority, debugName, waitForAllBarriers);
        } else {
            return new OrderedDrain(conveyor, ordinal, priority, debugName, comparator);
        }
    }

    private abstract static class InboundEdgeStreamBase implements InboundEdgeStream {
        final ProgressTracker tracker = new ProgressTracker();
        final ConcurrentConveyor<Object> conveyor;
        final int ordinal;
        final int priority;
        final ILogger logger;

        private InboundEdgeStreamBase(
                @Nonnull ConcurrentConveyor<Object> conveyor, int ordinal, int priority, @Nonnull String debugName) {
            this.conveyor = conveyor;
            this.ordinal = ordinal;
            this.priority = priority;
            logger = prefixedLogger(Logger.getLogger(getClass()), debugName);
            if (logger.isFinestEnabled()) {
                logger.finest("Coalescing " + conveyor.queueCount() + " input queues");
            }
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
        public boolean isDone() {
            return conveyor.liveQueueCount() == 0;
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

    /**
     * This implementation performs a single {@code drainTo} operation on all
     * the input queues queues and forwards the data to the destination, while
     * handling watermarks & barriers.
     */
    private static final class RoundRobinDrain extends InboundEdgeStreamBase {
        private final ItemDetector itemDetector = new ItemDetector();
        private final WatermarkCoalescer watermarkCoalescer;
        private final BitSet receivedBarriers; // indicates if current snapshot is received on the queue
        // Tells whether we are operating in exactly-once or at-least-once mode.
        // In other words, whether a barrier from all queues must be present before
        // draining more items from a queue where a barrier has been reached.
        // Once a terminal snapshot barrier is reached, this is always true.
        private boolean waitForAllBarriers;
        private SnapshotBarrier currentBarrier;  // next snapshot barrier to emit

        RoundRobinDrain(
                @Nonnull ConcurrentConveyor<Object> conveyor,
                int ordinal,
                int priority,
                @Nonnull String debugName,
                boolean waitForAllBarriers
        ) {
            super(conveyor, ordinal, priority, debugName);

            this.waitForAllBarriers = waitForAllBarriers;
            watermarkCoalescer = WatermarkCoalescer.create(conveyor.queueCount());
            receivedBarriers = new BitSet(conveyor.queueCount());
        }

        @Nonnull @Override
        public ProgressState drainTo(@Nonnull Predicate<Object> dest) {
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
                            logger.finest("Queue " + queueIndex + " is done, forwarding "
                                    + new Watermark(wmTimestamp));
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

        /**
         * Drains the supplied queue into a {@code dest} collection, up to the next
         * {@link Watermark} or {@link SnapshotBarrier}. Also updates the {@code tracker} with new status.
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

        private boolean maybeEmitWm(long timestamp, Predicate<Object> dest) {
            if (timestamp != NO_NEW_WM) {
                boolean res = dest.test(new Watermark(timestamp));
                assert res : "test result expected to be true";
                return true;
            }
            return false;
        }

        @Override
        public long topObservedWm() {
            return watermarkCoalescer.topObservedWm();
        }

        @Override
        public long coalescedWm() {
            return watermarkCoalescer.coalescedWm();
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
    }

    /**
     * This implementation asserts that the inputs are ordered according to the
     * supplied {@code Comparator} and merges them into one output stream while
     * preserving the order. Currently doesn't handle watermarks or barriers.
     */
    private static final class OrderedDrain extends InboundEdgeStreamBase {
        private final Comparator<Object> comparator;

        private final List<QueuedPipe<Object>> queues;
        private final List<ArrayDeque<Object>> drainedItems;
        private Object lastItem;
        private int lastMinIndex;

        @SuppressWarnings("unchecked")
        OrderedDrain(
                @Nonnull ConcurrentConveyor<Object> conveyor,
                int ordinal,
                int priority,
                @Nonnull String debugName,
                @Nullable ComparatorEx<?> comparator
        ) {
            super(conveyor, ordinal, priority, debugName);

            this.comparator = (Comparator<Object>) comparator;
            drainedItems = new ArrayList<>(conveyor.queueCount());
            queues = new ArrayList<>(conveyor.queueCount());
            for (int i = 0; i < conveyor.queueCount(); i++) {
                QueuedPipe<Object> q = conveyor.queue(i);
                drainedItems.add(new ArrayDeque<>(q.capacity()));
                queues.add(q);
            }
        }

        @Nonnull @Override
        public ProgressState drainTo(@Nonnull Predicate<Object> dest) {
            tracker.reset();
            tracker.notDone();

            // drain queues that are fully consumed
            for (int i = 0; i < queues.size(); i++) {
                if (drainedItems.get(i).isEmpty()) {
                    queues.get(i).drainTo(drainedItems.get(i), Integer.MAX_VALUE);
                }
            }

            outer:
            for (;;) {
                // find the current minimum item at the tail of queues
                Object minItem = null;
                for (int i = 0; i < drainedItems.size(); i++) {
                    Object item = drainedItems.get(i).peek();
                    if (item == null) {
                        // this queue doesn't have data and isn't done, we can't proceed
                        return tracker.toProgressState();
                    }
                    tracker.madeProgress();
                    if (item == DONE_ITEM) {
                        // queue is done, try again with it removed
                        queues.remove(i);
                        drainedItems.remove(i);
                        if (queues.isEmpty()) {
                            tracker.done();
                            return tracker.toProgressState();
                        }
                        continue outer;
                    }
                    if (item instanceof Watermark || item instanceof SnapshotBarrier) {
                        throw new JetException("Unexpected item observed: " + item);
                    }
                    if (minItem == null || comparator.compare(minItem, item) > 0) {
                        minItem = item;
                        lastMinIndex = i;
                    }
                }
                // return the item
                drainedItems.get(lastMinIndex).remove();
                assert lastItem == null || comparator.compare(lastItem, minItem) <= 0 :
                        "Disorder on a monotonicOrder edge";
                lastItem = minItem;
                boolean consumeResult = dest.test(lastItem);
                assert consumeResult : "consumeResult is false";
            }
        }

        @Override
        public long topObservedWm() {
            return Long.MIN_VALUE;
        }

        @Override
        public long coalescedWm() {
            return Long.MIN_VALUE;
        }

        @Override
        public boolean isDone() {
            return queues.isEmpty();
        }
    }
}
