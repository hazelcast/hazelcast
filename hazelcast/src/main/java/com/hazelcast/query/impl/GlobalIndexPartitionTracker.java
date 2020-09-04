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

    private static final long VERSION_INITIAL = 0;

    /** Lock to serialize updates to the state. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Number of partitions in the cluster. */
    private final int partitionCount;

    /** Current state. */
    private final AtomicReference<State> state;

    public GlobalIndexPartitionTracker(int partitionCount) {
        this.partitionCount = partitionCount;

        state = new AtomicReference<>(new State(VERSION_INITIAL, new PartitionIdSet(partitionCount), 0));
    }

    public Long getPartitionStamp(PartitionIdSet expectedPartitionIds) {
        State state0 = state.get();

        if (state0.pending > 0 || !state0.indexedPartitions.equals(expectedPartitionIds)) {
            return null;
        }

        return state0.version;
    }

    public boolean validatePartitionStamp(long version) {
        return state.get().version == version;
    }

    public boolean isMarked(int partitionId) {
        return state.get().indexedPartitions.contains(partitionId);
    }

    public int markedCount() {
        return state.get().indexedPartitions.size();
    }

    public void beginPartitionUpdate() {
        lock.lock();

        try {
            State oldState = state.get();

            State newState = new State(
                oldState.version + 1,
                oldState.indexedPartitions,
                oldState.pending + 1
            );

            state.set(newState);
        } finally {
            lock.unlock();
        }
    }

    public void mark(int partition) {
        complete(partition, true);
    }

    public void unmark(int partition) {
        complete(partition, false);
    }

    private void complete(int partition, boolean mark) {
        lock.lock();

        try {
            State oldState = state.get();

            assert oldState.pending > 0;

            PartitionIdSet newIndexedPartitions = oldState.indexedPartitions.copy();

            if (mark) {
                newIndexedPartitions.add(partition);
            } else {
                newIndexedPartitions.remove(partition);
            }

            State newState = new State(
                oldState.version + 1,
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
                oldState.version + 1,
                new PartitionIdSet(partitionCount),
                0
            );

            state.set(newState);
        } finally {
            lock.unlock();
        }
    }

    /**
     * State of the indexed partitions.
     */
    private static final class State {
        /** Monotonically increasing version, that is incremented on every partition info update. */
        private final long version;

        /** Partitions that are currently indexed. */
        private final PartitionIdSet indexedPartitions;

        /** The number of partitions that are being updated at the moment (indexing or deindexing).  */
        private final int pending;

        private State(long version, PartitionIdSet indexedPartitions, int pending) {
            this.version = version;
            this.indexedPartitions = indexedPartitions;
            this.pending = pending;
        }
    }
}
