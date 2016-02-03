/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.eviction;

/**
 * Interface for store implementations that holds {@link Evictable} entries to evict.
 *
 * @param <A> Type of the accessor (id) of the {@link com.hazelcast.internal.eviction.EvictionCandidate}
 * @param <E> Type of the {@link com.hazelcast.internal.eviction.Evictable} value of
 *            {@link com.hazelcast.internal.eviction.EvictionCandidate}
 */
public interface EvictableStore<A, E extends Evictable> {

    /**
     * The evict method is called by the {@link EvictionStrategy} to eventually evict, by the policy, selected
     * candidates from the internal data structures.
     *
     * @param evictionCandidates Multiple {@link EvictionCandidate} to be evicted
     * @param evictionListener   {@link EvictionListener} to listen evicted entries
     *
     * @return evicted entry count
     */
    <C extends EvictionCandidate<A, E>> int evict(Iterable<C> evictionCandidates,
                                                  EvictionListener<A, E> evictionListener);

}
