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

package com.hazelcast.cache.impl.eviction.impl.sampling;

import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictableStore;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;

/**
 * This interface is an extension of the {@link com.hazelcast.cache.impl.eviction.EvictableStore} interface
 * to add methods required for sampling based {@link com.hazelcast.cache.impl.eviction.EvictionStrategy}.
 *
 * @param <A> accessor (key) type of the evictable entry
 * @param <E> {@link com.hazelcast.cache.impl.eviction.Evictable} type (value) of the entry
 */
public interface SampleableEvictableStore<A, E extends Evictable>
        extends EvictableStore<A, E> {

    /**
     * The sample method is used to sample a number of entries (defined by the samples parameter) from
     * the internal data structure. This method should be executed in a constant time to deliver predictable
     * timing results of the eviction system.
     *
     * @param sampleCount Entry count to be sampled
     * @return Multiple {@link EvictionCandidate}s to be evicted
     */
    Iterable<EvictionCandidate<A, E>> sample(int sampleCount);

    /**
     * Some implementations might hold internal state whenever {@link #sample(int)} is called.
     * This method can be used to cleanup internal state after eviction happened. If no internal
     * state was necessary this method's body can be left empty.<br/>
     * {@link com.hazelcast.cache.impl.eviction.EvictionStrategy} implementations backed by a
     * {@link SampleableEvictableStore} are required to call this method as the last operation
     * inside the <tt>process</tt> method, directly before returning the resulting boolean.
     *
     * @param candidates The candidates that were sampled for a possible eviction
     */
    void cleanupSampling(Iterable<EvictionCandidate<A, E>> candidates);
}
