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

package com.hazelcast.jet.core;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.ObjLongBiFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A utility to help emitting {@link Watermark}s from a source which reads
 * events from multiple external partitions.
 *
 * <h3>The problems</h3>
 * <h4>1. Reading partition by partition</h4>
 * On restart it can happen that <em>partition1</em> has one very recent event
 * and <em>partition2</em> has one old event. If <em>partition1</em> is polled
 * first and the event is emitted, it will advance the watermark. Then later
 * <em>partition2</em> is polled and its event might be dropped as late.
 *
 * This utility helps you track watermarks per partition and decide when to
 * emit it.
 *
 * <h4>2. Some partition having no data</h4>
 * It can happen that some partition does not have any events at all and others
 * do. Or that the processor is not assigned any external partitions. In this
 * both cases no watermarks will be emitted. This utility supports <em>idle
 * timeout</em>: if some partition does not have any event during this time,
 * it will be marked as <em>idle</em>. If all partitions are idle or there are
 * no partitions, special <em>idle message</em> will be emitted and the
 * downstream will exclude this processor from watermark coalescing.
 *
 * <h3>Usage</h3>
 * API is designed to be used as a flat-mapping step in {@link Traverser}. Your
 * source might follow this pattern:
 *
 * <pre>{@code
 *   public boolean complete() {
 *       if (traverser == null) {
 *           List<Record> records = poll(); // get a batch of items from external source
 *           if (records.isEmpty()) {
 *               traverser = watermarkSourceUtil.handleNoEvent();
 *           } else {
 *               traverser = traverserIterable(records)
 *                   .flatMap(item -> watermarkSourceUtil.handleEvent(item, item.getPartition()));
 *           }
 *           traverser = traverser.onFirstNull(() -> traverser = null);
 *       }
 *       emitFromTraverser(traverser, item -> {
 *           if (!(item instanceof Watermark)) {
 *               // store your offset after item was emitted
 *               offsetsMap.put(item.getPartition(), item.getOffset());
 *           }
 *       });
 *       return false;
 *   }
 * }</pre>
 *
 * Other methods:
 * <ul>
 *     <li>Call {@link #increasePartitionCount} to set your partition count
 *     initially or whenever the count increases.
 *
 *     <li>If you support state snapshots, save the value returned by {@link
 *     #getWatermark} for all partitions to the snapshot. When restoring the
 *     state, call {@link #restoreWatermark}.<br>
 *
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
public class WatermarkSourceUtil<T> {

    private static final WatermarkPolicy[] EMPTY_WATERMARK_POLICIES = {};
    private static final long[] EMPTY_LONGS = {};

    private final long idleTimeoutNanos;
    private final ToLongFunction<? super T> timestampFn;
    private final Supplier<WatermarkPolicy> newWmPolicyFn;
    private final ObjLongBiFunction<? super T, ?> wrapFn;
    private final WatermarkEmissionPolicy wmEmitPolicy;
    private final AppendableTraverser<Object> traverser = new AppendableTraverser<>(2);

    private WatermarkPolicy[] wmPolicies = EMPTY_WATERMARK_POLICIES;
    private long[] watermarks = EMPTY_LONGS;
    private long[] markIdleAt = EMPTY_LONGS;
    private long lastEmittedWm = Long.MIN_VALUE;
    private long topObservedWm = Long.MIN_VALUE;
    private boolean allAreIdle;

    /**
     * A constructor.
     * <p>
     * The partition count is initially set to 0, call {@link
     * #increasePartitionCount} to set it.
     **/
    public WatermarkSourceUtil(WatermarkGenerationParams<? super T> params) {
        this.idleTimeoutNanos = MILLISECONDS.toNanos(params.idleTimeoutMillis());
        this.timestampFn = params.timestampFn();
        this.wrapFn = params.wrapFn();
        this.newWmPolicyFn = params.newWmPolicyFn();
        this.wmEmitPolicy = params.wmEmitPolicy();
    }

    /**
     * Flat-maps the given {@code item} by (possibly) prepending it with a
     * watermark. Designed to use when emitting from traverser:
     * <pre>{@code
     *     Traverser t = traverserIterable(...)
     *         .flatMap(item -> watermarkSourceUtil.flatMap(item, item.getPartition()));
     * }</pre>
     */
    @Nonnull
    public Traverser<Object> handleEvent(T item, int partitionIndex) {
        return handleEvent(System.nanoTime(), item, partitionIndex);
    }

    /**
     * Call this method when there is no event coming. It returns a traverser
     * with 0 or 1 object (the watermark). If you need just the Watermark, call
     * {@code next()} on the result.
     */
    @Nonnull
    public Traverser<Object> handleNoEvent() {
        return handleEvent(System.nanoTime(), null, -1);
    }

    // package-visible for tests
    Traverser<Object> handleEvent(long now, @Nullable T item, int partitionIndex) {
        assert traverser.isEmpty() : "the traverser returned previously not yet drained: remove all " +
                "items from the traverser before you call this method again.";
        if (item != null) {
            handleEventInt(now, partitionIndex, item);
        } else {
            handleNoEventInt(now);
        }
        if (item != null) {
            traverser.append(wrapFn.apply(item, timestampFn.applyAsLong(item)));
        }
        return traverser;
    }

    private void handleEventInt(long now, int partitionIndex, @Nonnull T event) {
        long eventTime = timestampFn.applyAsLong(event);
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

        if (wmEmitPolicy.shouldEmit(min, lastEmittedWm)) {
            traverser.append(new Watermark(min));
            lastEmittedWm = min;
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
