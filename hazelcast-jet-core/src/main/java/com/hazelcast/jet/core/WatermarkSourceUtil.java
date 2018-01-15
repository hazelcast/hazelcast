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

import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;

import java.util.Arrays;

import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A utility to help emitting {@link Watermark} from a source. It is useful if
 * the source reads events from multiple partitions.
 *
 * <h3>The problem</h3>
 * On restart it can happen that partition1 has one very recent event and
 * partition2 has one old event. If partition1 is checked first and the event
 * emitted, it will advance the watermark. Then, partition2 is checked and its
 * event might be dropped as late.
 *
 * This utility helps you track watermarks per partition and decide when to
 * emit it.
 *
 * <h3>Usage</h3>
 * <ul>
 *     <li>Call {@link #increasePartitionCount} to set your partition count.
 *
 *     <li>For each event you receive call {@link #handleEvent} method. If it
 *     returns a watermark, emit it <em>before</em> the event itself.
 *
 *     <li>If you didn't emit an event for some time (~100-1000ms), call {@link
 *     #handleNoEvent}. If it returns a watermark, emit it.
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

    private final long idleTimeoutNanos;
    private final DistributedToLongFunction<T> getTimestampF;
    private final DistributedSupplier<WatermarkPolicy> newWmPolicyF;
    private final WatermarkEmissionPolicy wmEmitPolicy;

    private WatermarkPolicy[] wmPolicies = {};
    private long[] watermarks = {};
    private long[] markIdleAt = {};
    private long lastEmittedWm = Long.MIN_VALUE;
    private boolean allAreIdle;

    /**
     * A constructor.
     * <p>
     * The partition count is initially set to 0, call {@link
     * #increasePartitionCount} to set it.
     **/
    public WatermarkSourceUtil(WatermarkGenerationParams<T> params) {
        this.idleTimeoutNanos = MILLISECONDS.toNanos(params.idleTimeoutMillis());
        this.getTimestampF = params.getTimestampF();
        this.newWmPolicyF = params.newWmPolicyF();
        this.wmEmitPolicy = params.wmEmitPolicy();
    }

    /**
     * Call this method after the event was emitted to decide if a watermark
     * should be sent after it.
     *
     * @param partitionIndex 0-based index of the source partition the event occurred in
     * @param event the event
     * @return watermark to emit before the event or {@code null}
     */
    public Watermark handleEvent(int partitionIndex, T event) {
        return handleEvent(System.nanoTime(), partitionIndex, event);
    }

    // package-visible for tests
    Watermark handleEvent(long now, int partitionIndex, T event) {
        long eventTime = getTimestampF.applyAsLong(event);
        watermarks[partitionIndex] = wmPolicies[partitionIndex].reportEvent(eventTime);
        markIdleAt[partitionIndex] = now + idleTimeoutNanos;
        allAreIdle = false;
        return handleNoEvent(now);
    }

    /**
     * Call this method when there are no observed events. Checks, if a
     * watermark should be emitted based on the passage of system time, which
     * could cause some partitions to become idle.
     *
     * @return watermark to emit or {@code null}
     */
    public Watermark handleNoEvent() {
        return handleNoEvent(System.nanoTime());
    }

    // package-visible for tests
    Watermark handleNoEvent(long now) {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < watermarks.length; i++) {
            if (idleTimeoutNanos > 0 && markIdleAt[i] <= now) {
                continue;
            }
            watermarks[i] = wmPolicies[i].getCurrentWatermark();
            min = Math.min(min, watermarks[i]);
        }

        if (min == Long.MAX_VALUE) {
            if (allAreIdle) {
                return null;
            }
            allAreIdle = true;
            return IDLE_MESSAGE;
        }

        if (!wmEmitPolicy.shouldEmit(min, lastEmittedWm)) {
            return null;
        }
        allAreIdle = false;
        lastEmittedWm = min;
        return new Watermark(min);
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
            wmPolicies[i] = newWmPolicyF.get();
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
