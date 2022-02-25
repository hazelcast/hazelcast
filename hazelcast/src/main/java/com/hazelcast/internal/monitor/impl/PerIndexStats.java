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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.query.impl.Comparison;
import com.hazelcast.query.impl.Index;

/**
 * Provides internal per-index statistics for {@link com.hazelcast.query.impl.Index
 * Index}.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:anoninnerlength"})
public interface PerIndexStats {

    /**
     * Empty no-op internal index stats instance.
     */
    PerIndexStats EMPTY = new PerIndexStats() {

        @Override
        public long makeTimestamp() {
            return 0;
        }

        @Override
        public long getCreationTime() {
            return 0;
        }

        @Override
        public long getQueryCount() {
            return 0;
        }

        @Override
        public void incrementQueryCount() {
            // do nothing
        }

        @Override
        public long getHitCount() {
            return 0;
        }

        @Override
        public long getTotalHitLatency() {
            return 0;
        }

        @Override
        public double getTotalNormalizedHitCardinality() {
            return 0.0;
        }

        @Override
        public long getInsertCount() {
            return 0;
        }

        @Override
        public long getTotalInsertLatency() {
            return 0;
        }

        @Override
        public long getUpdateCount() {
            return 0;
        }

        @Override
        public long getTotalUpdateLatency() {
            return 0;
        }

        @Override
        public long getRemoveCount() {
            return 0;
        }

        @Override
        public long getTotalRemoveLatency() {
            return 0;
        }

        @Override
        public long getMemoryCost() {
            return 0;
        }

        @Override
        public void onInsert(long timestamp, IndexOperationStats operationStats, Index.OperationSource operationSource) {
            // do nothing
        }

        @Override
        public void onUpdate(long timestamp, IndexOperationStats operationStats, Index.OperationSource operationSource) {
            // do nothing
        }

        @Override
        public void onRemove(long timestamp, IndexOperationStats operationStats, Index.OperationSource operationSource) {
            // do nothing
        }

        @Override
        public void onClear() {
            // do nothing
        }

        @Override
        public void onIndexHit(long timestamp, long hitCardinality) {
            // do nothing
        }

        @Override
        public void resetPerQueryStats() {
            // do nothing
        }

        @Override
        public MemoryAllocator wrapMemoryAllocator(MemoryAllocator memoryAllocator) {
            return memoryAllocator;
        }

        @Override
        public IndexOperationStats createOperationStats() {
            return IndexOperationStats.EMPTY;
        }

    };

    /**
     * Returns a new timestamp.
     * <p>
     * Used for latency measurement, expressed in nanoseconds.
     */
    long makeTimestamp();

    /**
     * Returns the creation time of the index.
     * <p>
     * The value is relative to midnight, January 1, 1970 UTC and expressed in
     * milliseconds.
     */
    long getCreationTime();

    /**
     * Returns the total number of queries served by the index.
     * <p>
     * The returned value may be less than the one returned by {@link
     * #getHitCount()} since a single query may hit the same index more than once.
     */
    long getQueryCount();

    /**
     * Increments the query count for the index.
     */
    void incrementQueryCount();

    /**
     * Returns the total number of hits into the index.
     * <p>
     * The returned value may be greater than the one returned by {@link
     * #getQueryCount} since a single query may hit the same index more than once.
     */
    long getHitCount();

    /**
     * Returns the total hit latency for the index.
     */
    long getTotalHitLatency();

    /**
     * Returns the total normalized cardinality of the hits served by the index.
     * <p>
     * Normalized hit cardinality is calculated as {@code hit_cardinality /
     * entry_count} at the time of the hit. The returned value is a sum of all
     * individual normalized hit cardinalities.
     */
    double getTotalNormalizedHitCardinality();

    /**
     * Returns the number of insert operations performed on the index.
     */
    long getInsertCount();

    /**
     * Returns the total latency (in nanoseconds) of insert operations performed
     * on the index.
     * <p>
     * To compute the average latency divide the returned value by {@link
     * #getInsertCount() insert operation count}.
     */
    long getTotalInsertLatency();

    /**
     * Returns the number of update operations performed on the index.
     */
    long getUpdateCount();

    /**
     * Returns the total latency (in nanoseconds) of update operations performed
     * on the index.
     * <p>
     * To compute the average latency divide the returned value by {@link
     * #getUpdateCount() update operation count}.
     */
    long getTotalUpdateLatency();

    /**
     * Returns the number of remove operations performed on the index.
     */
    long getRemoveCount();

    /**
     * Returns the total latency (in nanoseconds) of remove operations performed
     * on the index.
     * <p>
     * To compute the average latency divide the returned value by {@link
     * #getRemoveCount() remove operation count}.
     */
    long getTotalRemoveLatency();

    /**
     * Returns the memory cost of the index in bytes.
     * <p>
     * Currently, for on-heap indexes (OBJECT and BINARY storages), the returned
     * value is just a best-effort approximation and doesn't indicate a precise
     * on-heap memory usage of the index.
     */
    long getMemoryCost();

    /**
     * Invoked by the associated index after every insert operation.
     *
     * @param timestamp       the time at which the insert operation was started.
     * @param operationStats  the operation stats to track the stats.
     * @param operationSource the operation source.
     * @see #makeTimestamp
     * @see com.hazelcast.query.impl.Index#putEntry
     */
    void onInsert(long timestamp, IndexOperationStats operationStats, Index.OperationSource operationSource);

    /**
     * Invoked by the associated index after every update operation.
     *
     * @param timestamp       the time at which the update operation was started.
     * @param operationStats  the operation stats to track the stats.
     * @param operationSource the operation source.
     * @see #makeTimestamp
     * @see com.hazelcast.query.impl.Index#putEntry
     */
    void onUpdate(long timestamp, IndexOperationStats operationStats, Index.OperationSource operationSource);

    /**
     * Invoked by the associated index after every remove operation.
     *
     * @param timestamp       the time at which the remove operation was started.
     * @param operationStats  the operation stats to track the stats.
     * @param operationSource the operation source.
     * @see #makeTimestamp
     * @see com.hazelcast.query.impl.Index#removeEntry
     */
    void onRemove(long timestamp, IndexOperationStats operationStats, Index.OperationSource operationSource);

    /**
     * Invoked by the associated index after the index was cleared.
     *
     * @see com.hazelcast.query.impl.Index#clear
     */
    void onClear();

    /**
     * Invoked by the associated index after every index hit.
     * <p>
     * Following operations generate a hit:
     * <ul>
     * <li>{@link com.hazelcast.query.impl.Index#getRecords(Comparable)}
     * <li>{@link com.hazelcast.query.impl.Index#getRecords(Comparable[])}
     * <li>{@link com.hazelcast.query.impl.Index#getRecords(Comparison, Comparable)}
     * <li>{@link com.hazelcast.query.impl.Index#getRecords(Comparable, boolean, Comparable, boolean)}
     * </ul>
     *
     * @param timestamp      the time at which the hit-producing operation was
     *                       started.
     * @param hitCardinality the cardinality of the hit.
     * @see #makeTimestamp()
     */
    void onIndexHit(long timestamp, long hitCardinality);

    /**
     * Resets the per-query stats, if any, currently tracked by this internal
     * index stats instance.
     */
    void resetPerQueryStats();

    /**
     * Wraps the given memory allocator.
     * <p>
     * Used for the off-heap memory cost tracking.
     *
     * @param memoryAllocator the memory allocator to wrap.
     * @return the wrapped memory allocator.
     */
    MemoryAllocator wrapMemoryAllocator(MemoryAllocator memoryAllocator);

    /**
     * Creates a new per-operation stats instance.
     *
     * @return the created per-operation stats instance.
     */
    IndexOperationStats createOperationStats();

}
