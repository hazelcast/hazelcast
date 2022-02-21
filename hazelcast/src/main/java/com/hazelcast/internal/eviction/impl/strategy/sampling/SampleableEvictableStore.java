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

package com.hazelcast.internal.eviction.impl.strategy.sampling;

import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.EvictableStore;
import com.hazelcast.internal.eviction.EvictionCandidate;

/**
 * Interface for sampleable store implementations that holds {@link Evictable} entries to evict.
 */
public interface SampleableEvictableStore<A, E extends Evictable> extends EvictableStore<A, E> {

    /**
     * The sample method is used to sample a number of entries (defined by the samples parameter) from
     * the internal data structure. This method should be executed in a constant time to deliver predictable
     * timing results of the eviction system.
     *
     * @param sampleCount Entry count to be sampled
     *
     * @return Multiple {@link EvictionCandidate} to be evicted
     */
    <C extends EvictionCandidate<A, E>> Iterable<C> sample(int sampleCount);

}
