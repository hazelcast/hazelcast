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

package com.hazelcast.query.impl;

import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.query.impl.GlobalIndexPartitionTracker.PartitionStamp;

/**
 * Provides the private index API.
 */
public interface InternalIndex extends Index {

    /**
     * Canonicalizes the given value for the purpose of a hash-based lookup.
     * <p>
     * The method is used while performing InPredicate queries to canonicalize
     * the set of values in question, so additional duplicate-eliminating
     * post-processing step can be avoided.
     *
     * @param value the value to canonicalize.
     * @return the canonicalized value.
     */
    Comparable canonicalizeQueryArgumentScalar(Comparable value);

    /**
     * Returns {@code true} if the given partition is indexed by this index,
     * {@code false} otherwise.
     */
    boolean hasPartitionIndexed(int partitionId);

    /**
     * Returns {@code true} if the number of indexed partitions is equal to {@code
     * ownedPartitionCount}, {@code false} otherwise.
     * <p>
     * The method is used to check whether a global index is still being constructed concurrently
     * so that some partitions are not indexed and query may suffer from entry misses.
     * If the index construction is still in progress, a query optimizer ignores the index.
     * <p>
     * The aforementioned race condition is not relevant to local off-heap indexes,
     * since index construction is performed in partition threads.
     *
     * @param ownedPartitionCount a count of owned partitions a query runs on.
     * Negative value indicates that the value is not defined.
     */
    boolean allPartitionsIndexed(int ownedPartitionCount);

    /**
     * Notifies the index that a partition update is about to begin. Could be caused be either
     * partition add (e.g. migration from another member, dynamic index creation), or partition
     * remove (e.g. migration to another member).
     * <p>
     * While in this state, the index cannot be queried by the SQL engine safely, because it
     * will produce inconsistent results.
     * <p>
     * Internally this call increments the counter of active partition updates. The counter
     * is decremented by subsequent calls to {@link #markPartitionAsIndexed(int)} or
     * {@link #markPartitionAsUnindexed(int)}. When the counter reaches zero, an index
     * could be queried again.
     */
    void beginPartitionUpdate();

    /**
     * Marks the given partition as indexed by this index.
     *
     * @param partitionId the ID of the partition to mark as indexed.
     */
    void markPartitionAsIndexed(int partitionId);

    /**
     * Marks the given partition as unindexed by this index.
     *
     * @param partitionId the ID of the partition to mark as unindexed.
     */
    void markPartitionAsUnindexed(int partitionId);

    /**
     * Returns the index stats associated with this index.
     */
    PerIndexStats getPerIndexStats();

    /**
     * Get a monotonically increasing stamp and the partition ID set currently
     * contained in the index. The received stamp is used later to verify that
     * no partition was added or removed by calling to {@link
     * #validatePartitionStamp(long)}.
     *
     * @return the stamp and the partitions, or {@code null}, if the index is
     *     currently being updated and cannot be safely read.
     */
    PartitionStamp getPartitionStamp();

    /**
     * Verifies that the given partition stamp is still valid. It is valid iff there were
     * no partition updates since the call to the {@link #getPartitionStamp()}
     * that produced this stamp.
     *
     * @param stamp the value of {@link PartitionStamp#stamp} field
     * @return {@code true} if the stamp is still valid, {@code false} otherwise
     */
    boolean validatePartitionStamp(long stamp);
}
