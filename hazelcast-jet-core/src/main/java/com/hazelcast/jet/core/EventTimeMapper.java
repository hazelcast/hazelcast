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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.ObjLongBiFunction;
import com.hazelcast.jet.pipeline.Sources;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.core.SlidingWindowPolicy.tumblingWinPolicy;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A utility to that helps a source emit events according to a given {@link
 * EventTimePolicy}. Generally this class should be used if a source needs
 * to emit {@link Watermark watermarks}. The mapper deals with the
 * following concerns:
 *
 * <h4>1. Reading partition by partition</h4>
 *
 * Upon restart it can happen that partition <em>partition1</em> has one
 * very recent event and <em>partition2</em> has an old one. If we poll
 * <em>partition1</em> first and emit its recent event, it will advance the
 * watermark. When we poll <em>partition2</em> later on, its event will be
 * behind the watermark and can be dropped as late. This utility tracks the
 * event timestamps for each partition individually and allows the
 * processor to emit the watermark that is correct with respect to all the
 * partitions.
 *
 * <h4>2. Some partition having no data</h4>
 *
 * It can happen that some partition does not have any events at all while
 * others do or the processor doesn't get any external partitions assigned
 * to it. If we simply wait for the timestamps in all partitions to advance
 * to some point, we won't be emitting any watermarks. This utility
 * supports the <em>idle timeout</em>: if there's no new data from a
 * partition after the timeout elapses, it will be marked as <em>idle</em>,
 * allowing the processor's watermark to advance as if that partition didn't
 * exist. If all partitions are idle or there are no partitions, the
 * processor will emit a special <em>idle message</em> and the downstream
 * will exclude this processor from watermark coalescing.
 *
 * <h4>3. Wrapping of events</h4>
 *
 * Events may need to be wrapped with the extracted timestamp if {@link
 * EventTimePolicy#wrapFn()} is set.
 *
 * <h4>4. Throttling of Watermarks</h4>
 *
 * Watermarks are only consumed by windowing operations and emitting
 * watermarks more frequently than the given {@link
 * EventTimePolicy#watermarkThrottlingFrameSize()} is wasteful since they
 * are broadcast to all processors. The mapper ensures that watermarks are
 * emitted as seldom as possible.
 *
 * <h3>Usage</h3>
 *
 * The API is designed to be used as a flat-mapping step in the {@link
 * Traverser} that holds the output data. Your source can follow this
 * pattern:
 *
 * <pre>{@code
 * public boolean complete() {
 *     if (traverser == null) {
 *         List<Record> records = poll();
 *         if (records.isEmpty()) {
 *             traverser = eventTimeMapper.flatMapIdle();
 *         } else {
 *             traverser = traverserIterable(records)
 *                 .flatMap(event -> eventTimeMapper.flatMapEvent(
 *                      event, event.getPartition()));
 *         }
 *         traverser = traverser.onFirstNull(() -> traverser = null);
 *     }
 *     emitFromTraverser(traverser, event -> {
 *         if (!(event instanceof Watermark)) {
 *             // store your offset after event was emitted
 *             offsetsMap.put(event.getPartition(), event.getOffset());
 *         }
 *     });
 *     return false;
 * }
 * }</pre>
 *
 * Other methods:
 * <ul><li>
 *     Call {@link #increasePartitionCount} to set your partition count
 *     initially or whenever the count increases.
 * <li>
 *     If you support state snapshots, save the value returned by {@link
 *     #getWatermark} for all partitions to the snapshot. When restoring the
 *     state, call {@link #restoreWatermark}.
 *     <br>
 *     You should save the value under your external partition key so that the
 *     watermark value can be restored to correct processor instance. The key
 *     should also be wrapped using {@link BroadcastKey#broadcastKey
 *     broadcastKey()}, because the external partitions don't match Hazelcast
 *     partitions. This way, all processor instances will see all keys and they
 *     can restore partition they handle and ignore others.
 * </ul>
 *
 * @param <T> event type
 */
public class EventTimeMapper<T> {

    /**
     * Value to use as the {@code nativeEventTime} argument when calling
     * {@link #flatMapEvent(Object, int, long)} whene there's no native event
     * time to supply.
     */
    public static final long NO_NATIVE_TIME = Long.MIN_VALUE;

    private static final WatermarkPolicy[] EMPTY_WATERMARK_POLICIES = {};
    private static final long[] EMPTY_LONGS = {};

    private final long idleTimeoutNanos;
    @Nullable
    private final ToLongFunction<? super T> timestampFn;
    private final Supplier<? extends WatermarkPolicy> newWmPolicyFn;
    private final ObjLongBiFunction<? super T, ?> wrapFn;
    @Nullable
    private final SlidingWindowPolicy watermarkThrottlingFrame;
    private final AppendableTraverser<Object> traverser = new AppendableTraverser<>(2);

    private WatermarkPolicy[] wmPolicies = EMPTY_WATERMARK_POLICIES;
    private long[] watermarks = EMPTY_LONGS;
    private long[] markIdleAt = EMPTY_LONGS;
    private long lastEmittedWm = Long.MIN_VALUE;
    private long topObservedWm = Long.MIN_VALUE;
    private boolean allAreIdle;

    /**
     * The partition count is initially set to 0, call
     * {@link #increasePartitionCount} to set it.
     *
     * @param eventTimePolicy event time policy as passed in {@link
     *                        Sources#streamFromProcessorWithWatermarks}
     **/
    public EventTimeMapper(EventTimePolicy<? super T> eventTimePolicy) {
        this.idleTimeoutNanos = MILLISECONDS.toNanos(eventTimePolicy.idleTimeoutMillis());
        this.timestampFn = eventTimePolicy.timestampFn();
        this.wrapFn = eventTimePolicy.wrapFn();
        this.newWmPolicyFn = eventTimePolicy.newWmPolicyFn();
        if (eventTimePolicy.watermarkThrottlingFrameSize() != 0) {
            this.watermarkThrottlingFrame = tumblingWinPolicy(eventTimePolicy.watermarkThrottlingFrameSize())
                    .withOffset(eventTimePolicy.watermarkThrottlingFrameOffset());
        } else {
            this.watermarkThrottlingFrame = null;
        }
    }

    /**
     * Flat-maps the given {@code event} by (possibly) prepending it with a
     * watermark. Designed to use when emitting from traverser:
     * <pre>{@code
     *     Traverser t = traverserIterable(...)
     *         .flatMap(event -> eventTimeMapper.flatMapEvent(
     *                 event, event.getPartition(), nativeEventTime));
     * }</pre>
     *
     * @param event           the event
     * @param partitionIndex  the source partition index the event came from
     * @param nativeEventTime native event time in case no {@code timestampFn} was supplied or
     *                        {@link #NO_NATIVE_TIME} if the event has no native timestamp
     */
    @Nonnull
    public Traverser<Object> flatMapEvent(T event, int partitionIndex, long nativeEventTime) {
        return flatMapEvent(System.nanoTime(), event, partitionIndex, nativeEventTime);
    }

    /**
     * Call this method when there is no event coming. It returns a traverser
     * with 0 or 1 object (the watermark).
     */
    @Nonnull
    public Traverser<Object> flatMapIdle() {
        return flatMapEvent(System.nanoTime(), null, -1, NO_NATIVE_TIME);
    }

    // package-visible for tests
    Traverser<Object> flatMapEvent(long now, @Nullable T event, int partitionIndex, long nativeEventTime) {
        assert traverser.isEmpty() : "the traverser returned previously not yet drained: remove all " +
                "items from the traverser before you call this method again.";
        if (event != null) {
            if (timestampFn == null && nativeEventTime == NO_NATIVE_TIME) {
                throw new JetException("Neither timestampFn nor nativeEventTime specified");
            }
            long eventTime = timestampFn == null ? nativeEventTime : timestampFn.applyAsLong(event);
            handleEventInt(now, partitionIndex, eventTime);
            traverser.append(wrapFn.apply(event, eventTime));
        } else {
            handleNoEventInt(now);
        }
        return traverser;
    }

    private void handleEventInt(long now, int partitionIndex, long eventTime) {
        wmPolicies[partitionIndex].reportEvent(eventTime);
        markIdleAt[partitionIndex] = now + idleTimeoutNanos;
        allAreIdle = false;
        handleNoEventInt(now);
    }

    private void handleNoEventInt(long now) {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < watermarks.length; i++) {
            if (idleTimeoutNanos > 0 && markIdleAt[i] <= now) {
                continue;
            }
            watermarks[i] = wmPolicies[i].getCurrentWatermark();
            topObservedWm = Math.max(topObservedWm, watermarks[i]);
            min = Math.min(min, watermarks[i]);
        }

        if (min == Long.MAX_VALUE) {
            if (allAreIdle) {
                return;
            }
            // we've just became fully idle. Forward the top WM now, if needed
            min = topObservedWm;
            allAreIdle = true;
        } else {
            allAreIdle = false;
        }

        if (min > lastEmittedWm) {
            long newWm = watermarkThrottlingFrame != null ? watermarkThrottlingFrame.floorFrameTs(min) : Long.MIN_VALUE;
            if (newWm > lastEmittedWm) {
                traverser.append(new Watermark(newWm));
                lastEmittedWm = newWm;
            }
        }
        if (allAreIdle) {
            traverser.append(IDLE_MESSAGE);
        }
    }

    /**
     * Changes the partition count. The new partition count must be higher or
     * equal to the current count.
     * <p>
     * You can call this method at any moment. Added partitions will be
     * considered <em>active</em> initially.
     *
     * @param newPartitionCount partition count, must be higher than the
     *                          current count
     */
    public void increasePartitionCount(int newPartitionCount) {
        increasePartitionCount(System.nanoTime(), newPartitionCount);
    }

    // package-visible for tests
    void increasePartitionCount(long now, int newPartitionCount) {
        int oldPartitionCount = wmPolicies.length;
        if (newPartitionCount < oldPartitionCount) {
            throw new IllegalArgumentException("partition count must increase. Old count=" + oldPartitionCount
                    + ", new count=" + newPartitionCount);
        }

        wmPolicies = Arrays.copyOf(wmPolicies, newPartitionCount);
        watermarks = Arrays.copyOf(watermarks, newPartitionCount);
        markIdleAt = Arrays.copyOf(markIdleAt, newPartitionCount);

        for (int i = oldPartitionCount; i < newPartitionCount; i++) {
            wmPolicies[i] = newWmPolicyFn.get();
            watermarks[i] = Long.MIN_VALUE;
            markIdleAt[i] = now + idleTimeoutNanos;
        }
    }

    /**
     * Watermark value to be saved to state snapshot for the given source
     * partition index. The returned value should be {@link
     * #restoreWatermark(int, long) restored} to a processor handling the same
     * partition after restart.
     * <p>
     * Method is meant to be used from {@link Processor#saveToSnapshot()}.
     *
     * @param partitionIndex 0-based source partition index.
     * @return A value to save to state snapshot
     */
    public long getWatermark(int partitionIndex) {
        return watermarks[partitionIndex];
    }

    /**
     * Restore watermark value from state snapshot.
     * <p>
     * Method is meant to be used from {@link
     * Processor#restoreFromSnapshot(Inbox)}.
     * <p>
     * See {@link #getWatermark(int)}.
     *
     * @param partitionIndex 0-based source partition index.
     * @param wm Watermark value to restore
     */
    public void restoreWatermark(int partitionIndex, long wm) {
        watermarks[partitionIndex] = wm;
    }
}
