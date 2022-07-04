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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.function.ObjLongBiFunction;
import com.hazelcast.jet.pipeline.Sources;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.jet.core.SlidingWindowPolicy.tumblingWinPolicy;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A utility that helps a source emit events according to a given {@link
 * EventTimePolicy}. Generally this class should be used if a source needs
 * to emit {@link Watermark watermarks}. The mapper deals with the
 * following concerns:
 *
 * <h4>1. Reading partition by partition</h4>
 *
 * Upon restart it can happen that partition <em>P1</em> has one very
 * recent event and <em>P2</em> has an old one. If we poll <em>P1</em>
 * first and emit its recent event, it will advance the watermark. When we
 * poll <em>P2</em> later on, its event will be behind the watermark and
 * can be dropped as late. This utility tracks the event timestamps for
 * each source partition individually and allows the processor to emit the
 * watermark that is correct with respect to all the partitions.
 *
 * <h4>2. Some partition having no data</h4>
 *
 * It can happen that some partition does not have any events at all while
 * others do, or the processor doesn't get any external partitions assigned
 * to it. If we simply wait for the timestamps in all partitions to advance
 * to some point, we won't be emitting any watermarks. This utility
 * supports the <em>idle timeout</em>: if there's no new data from a
 * partition after the timeout elapses, it will be marked as <em>idle</em>,
 * allowing the processor's watermark to advance as if that partition
 * didn't exist. If all partitions are idle or there are no partitions, the
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
 * emitted according to the throttling frame size.
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
 *             traverser = traverseIterable(records)
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
 *     Call {@link #addPartitions} and {@link #removePartition} to change your
 *     partition count initially or whenever the count changes.
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
 *     can restore the partitions they handle and ignore others.
 * </ul>
 *
 * @param <T> the event type
 *
 * @since Jet 3.0
 */
public class EventTimeMapper<T> {

    /**
     * Value to use as the {@code nativeEventTime} argument when calling
     * {@link #flatMapEvent(Object, int, long)} when there's no native event
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
     * The partition count is initially set to 0, call {@link #addPartitions}
     * to add partitions.
     *
     * @param eventTimePolicy event time policy as passed in {@link
     *                        Sources#streamFromProcessorWithWatermarks}
     */
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
     * @param event           the event to flat-map.
     *                        If {@code null}, it's equivalent to the behavior of {@link #flatMapIdle()}
     * @param partitionIndex  the source partition index the event came from
     * @param nativeEventTime native event time in case no {@code timestampFn} was supplied or
     *                        {@link #NO_NATIVE_TIME} if the event has no native timestamp
     * @return a traverser over the given event and the watermark (if it was due)
     */
    @Nonnull
    public Traverser<Object> flatMapEvent(@Nullable T event, int partitionIndex, long nativeEventTime) {
        return flatMapEvent(System.nanoTime(), event, partitionIndex, nativeEventTime);
    }

    /**
     * Call this method when there is no event to emit. It returns a traverser
     * over the watermark, if it was due.
     */
    @Nonnull
    public Traverser<Object> flatMapIdle() {
        return flatMapEvent(System.nanoTime(), null, -1, NO_NATIVE_TIME);
    }

    /**
     * A lower-level variant of {@link #flatMapEvent(Object, int, long)
     * flatMapEvent(T, int, long)} that accepts an explicit result of a
     * {@code System.nanoTime()} call. Use this variant if you're calling it in
     * a hot loop, in order to avoid repeating the expensive
     * {@code System.nanoTime()} call.
     */
    public Traverser<Object> flatMapEvent(long now, @Nullable T event, int partitionIndex, long nativeEventTime) {
        assert traverser.isEmpty() : "the traverser returned previously not yet drained: remove all " +
                "items from the traverser before you call this method again.";
        if (event == null) {
            handleNoEventInternal(now, Long.MAX_VALUE);
            return traverser;
        }
        long eventTime;
        if (timestampFn != null) {
            eventTime = timestampFn.applyAsLong(event);
        } else {
            eventTime = nativeEventTime;
            if (eventTime == NO_NATIVE_TIME) {
                throw new JetException("Neither timestampFn nor nativeEventTime specified");
            }
        }
        handleEventInternal(now, partitionIndex, eventTime);
        return traverser.append(wrapFn.apply(event, eventTime));
    }

    private void handleEventInternal(long now, int partitionIndex, long eventTime) {
        wmPolicies[partitionIndex].reportEvent(eventTime);
        markIdleAt[partitionIndex] = now + idleTimeoutNanos;
        allAreIdle = false;
        // Some WM policies use the system time to determine the watermark. This
        // opens the door to race conditions, where we first use the current time
        // to assign the event time and then check the current time again to
        // determine the watermark. If enough time passes between these two calls,
        // the event would be artificially rendered late. Therefore we cap the WM
        // to this event's timestamp, while still not allowing it to go back.
        handleNoEventInternal(now, eventTime);
    }

    private void handleNoEventInternal(long now, long maxWmValue) {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < watermarks.length; i++) {
            if (idleTimeoutNanos > 0 && markIdleAt[i] <= now) {
                continue;
            }
            // the new watermark must not be less than the previous watermark and not more than maxWmValue
            watermarks[i] = Math.max(watermarks[i], Math.min(wmPolicies[i].getCurrentWatermark(), maxWmValue));
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
     * Adds {@code addedCount} partitions. Added partitions will be initially
     * considered <em>active</em> and having watermark value equal to the last
     * emitted watermark. Their indices will follow current highest index.
     * <p>
     * You can call this method whenever new partitions are detected.
     *
     * @param addedCount number of added partitions, must be >= 0
     */
    public void addPartitions(int addedCount) {
        addPartitions(System.nanoTime(), addedCount);
    }

    // package-visible for tests
    void addPartitions(long now, int addedCount) {
        int oldPartitionCount = wmPolicies.length;
        int newPartitionCount = oldPartitionCount + checkNotNegative(addedCount, "addedCount must be >= 0");

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
     * Removes a partition that will no longer have events. If we were waiting
     * for a watermark from it, the returned traverser might contain a
     * watermark to emit.
     * <p>
     * Note that the indexes of partitions with index larger than the given
     * {@code partitionIndex} will be decremented by 1.
     *
     * @param partitionIndex the index of the removed partition
     */
    public Traverser<Object> removePartition(int partitionIndex) {
        return removePartition(System.nanoTime(), partitionIndex);
    }

    // package-visible for tests
    Traverser<Object> removePartition(long now, int partitionIndex) {
        wmPolicies = arrayRemove(wmPolicies, partitionIndex);
        watermarks = arrayRemove(watermarks, partitionIndex);
        markIdleAt = arrayRemove(markIdleAt, partitionIndex);
        handleNoEventInternal(now, Long.MAX_VALUE);
        return traverser;
    }

    private static long[] arrayRemove(long[] array, int partitionIndex) {
        long[] res = new long[array.length - 1];
        System.arraycopy(array, 0, res, 0, partitionIndex);
        System.arraycopy(array, partitionIndex + 1, res, partitionIndex, res.length - partitionIndex);
        return res;
    }

    private static <T> T[] arrayRemove(T[] array, int partitionIndex) {
        T[] res = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length - 1);
        System.arraycopy(array, 0, res, 0, partitionIndex);
        System.arraycopy(array, partitionIndex + 1, res, partitionIndex, res.length - partitionIndex);
        return res;
    }

    /**
     * Returns the current partition count.
     */
    public int partitionCount() {
        return wmPolicies.length;
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
     * @param wm watermark value to restore
     */
    public void restoreWatermark(int partitionIndex, long wm) {
        watermarks[partitionIndex] = wm;
        lastEmittedWm = Long.MAX_VALUE;
        for (long watermark : watermarks) {
            lastEmittedWm = Math.min(watermark, lastEmittedWm);
        }
    }
}
