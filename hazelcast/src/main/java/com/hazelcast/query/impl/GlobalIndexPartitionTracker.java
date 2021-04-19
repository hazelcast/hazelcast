/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.internal.util.collection.PartitionIdSet;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tracker of indexed partition for global indexes.
 * <p>
 * Used by {@link InternalIndex} to keep track of indexed partitions and decide whether the query
 * using this index could be executed or not.
 */
public class GlobalIndexPartitionTracker {

    public static final long STAMP_INVALID = -1;
    private static final long STAMP_INITIAL = 0;

    /**
     * Lock to serialize updates to the state.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Number of partitions in the cluster.
     */
    private final int partitionCount;

    /**
     * Current state.
     */
    private final AtomicReference<State> state;

    public GlobalIndexPartitionTracker(int partitionCount) {
        this.partitionCount = partitionCount;

        state = new AtomicReference<>(new State(STAMP_INITIAL, new PartitionIdSet(partitionCount), 0));
    }

    /**
     * Gets the stamp associated with the given expected partition IDs.
     * <p>
     * The obtained stamp could be checked for validity using {@link #validatePartitionStamp(long)}.
     *
     * @param expectedPartitionIds expected partition IDs
     * @return stamp or {@code -1} if indexed partitions do not match expected partitions, or there is
     * an active partition update from {@link #beginPartitionUpdate()}
     */
    public long getPartitionStamp(PartitionIdSet expectedPartitionIds) {
        State state0 = state.get();

        if (state0.pending > 0 || !state0.indexedPartitions.equals(expectedPartitionIds)) {
            return STAMP_INVALID;
        }

        return state0.stamp;
    }

    /**
     * Validates the stamp obtained from the previous call to {@link #getPartitionStamp(PartitionIdSet)}.
     * <p>
     * The stamp is valid iff:
     * <ul>
     *     <li>The index still has the same set of indexed partitions, as was expected by the previous call
     *     to the {@link #getPartitionStamp(PartitionIdSet)} that returned this stamp
     *     <li>There are no active partition updates
     * </ul>
     *
     * @param stamp stamp
     * @return {@code true} if the stamp is valid, {@code false} otherwise
     */
    public boolean validatePartitionStamp(long stamp) {
        return state.get().stamp == stamp;
    }

    public boolean isIndexed(int partitionId) {
        return state.get().indexedPartitions.contains(partitionId);
    }

    public int indexedCount() {
        return state.get().indexedPartitions.size();
    }

    public void beginPartitionUpdate() {
        lock.lock();

        try {
            State oldState = state.get();

            State newState = new State(
                    oldState.stamp + 1,
                    oldState.indexedPartitions,
                    oldState.pending + 1
            );

            state.set(newState);
        } finally {
            lock.unlock();
        }
    }

    public void partitionIndexed(int partition) {
        complete(partition, true);
    }

    public void partitionUnindexed(int partition) {
        complete(partition, false);
    }

    private void complete(int partition, boolean indexed) {
        lock.lock();

        try {
            State oldState = state.get();

            assert oldState.pending > 0;

            PartitionIdSet newIndexedPartitions = oldState.indexedPartitions.copy();

            if (indexed) {
                newIndexedPartitions.add(partition);
            } else {
                newIndexedPartitions.remove(partition);
            }

            State newState = new State(
                    oldState.stamp + 1,
                    newIndexedPartitions,
                    oldState.pending - 1
            );

            state.set(newState);
        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        lock.lock();

        try {
            State oldState = state.get();

            State newState = new State(
                    oldState.stamp + 1,
                    new PartitionIdSet(partitionCount),
                    0
            );

            state.set(newState);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "GlobalIndexPartitionTracker{"
                + "partitionCount=" + partitionCount
                + ", state=" + state + '}';
    }

    /**
     * State of the indexed partitions.
     */
    private static final class State {
        /**
         * Monotonically increasing stamp, that is incremented on every partition info update.
         */
        private final long stamp;

        /**
         * Partitions that are currently indexed.
         */
        private final PartitionIdSet indexedPartitions;

        /**
         * The number of partitions that are being updated at the moment (indexing or deindexing).
         */
        private final int pending;

        private State(long stamp, PartitionIdSet indexedPartitions, int pending) {
            this.stamp = stamp;
            this.indexedPartitions = indexedPartitions;
            this.pending = pending;
        }

        @Override
        public String toString() {
            return "State{"
                    + "stamp=" + stamp
                    + ", indexedPartitions={size="+indexedPartitions.size()
                    + ", partitions="+ indexedPartitions +"}"
                    + ", pending=" + pending
                    + '}';
        }
    }
}
