/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.eviction;

/**
 * An {@link com.hazelcast.cache.impl.eviction.EvictableStore} implementation is a storage for
 * evictable entries such as records.
 *
 * @param <A> accessor (key) type of the evictable entry
 * @param <E> {@link com.hazelcast.cache.impl.eviction.Evictable} type (value) of the entry
 */
public interface EvictableStore<A, E extends Evictable> {

    /**
     * The evict method is called by the {@link EvictionStrategy} to eventually evict,
     * by the {@link EvictionPolicyStrategy}, selected {@link EvictionCandidate}s from the internal
     * data structures.
     *
     * @param evictionCandidates Multiple {@link EvictionCandidate} to be evicted
     * @return The count of evicted entries
     */
    int evict(Iterable<EvictionCandidate<A, E>> evictionCandidates);

    /**
     * Returns the current count of the partition
     *
     * @return The current number of entries in the partition
     */
    long getPartitionEntryCount();

    /**
     * Returns the defined capacity over all partitions in the cluster for that data structure.
     * This is assumed to be equal to <tt>max := sum(partitionSize)</tt> in partitioned
     * data structures.
     *
     * @return The globally set cluster-wide capacity of the storage
     */
    long getGlobalEntryCapacity();

    /**
     * Returns the memory consumption of the current partition in bytes. This is an optional feature.
     * If the underlying storage engine doesn't support that feature it has to return <tt>-1</tt>.
     *
     * @return The current memory consumption of the storage in bytes or <tt>-1</tt> if this feature
     * is not supported
     */
    long getPartitionMemoryConsumption();
}
