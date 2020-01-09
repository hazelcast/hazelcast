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

import com.hazelcast.internal.monitor.impl.PerIndexStats;

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
     * Returns {@code true} if all {@code queryPartitions} are indexed,
     * {@code false} otherwise.
     * <p>
     * The method is used to check whether a global index is still being constructed concurrently
     * so that some partitions are not indexed and query may suffer from entry misses.
     * If the index construction is still in progress, a query optimizer ignores the index.
     * <p>
     * The aforementioned race condition is not relevant to local off-heap indexes,
     * since index construction is performed in partition-threads.
     * @param ownedPartitionCount a count of owned partitions a query runs on.
     * Negative value indicates that the value is not defined.
     */
    boolean allPartitionsIndexed(int ownedPartitionCount);

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

}
