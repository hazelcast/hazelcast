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

package com.hazelcast.cache.impl.eviction.impl.strategy.sampling;

import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;
import com.hazelcast.cache.impl.eviction.EvictionPolicyEvaluator;
import com.hazelcast.cache.impl.eviction.EvictionStrategy;
import com.hazelcast.cache.impl.eviction.impl.strategy.AbstractEvictionStrategy;

/**
 * Sampling based {@link EvictionStrategy} implementation.
 * This strategy select sample {@link Evictable} entries from {@link SampleableEvictableStore}.
 */
public class SamplingBasedEvictionStrategy<A, E extends Evictable, S extends SampleableEvictableStore<A, E>>
        extends AbstractEvictionStrategy<A, E, S> {

    private static final int SAMPLE_COUNT = 15;

    /**
     * Processes sampling based eviction logic on {@link SampleableEvictableStore}.
     *
     * @param sampleableEvictableStore  {@link SampleableEvictableStore} that holds {@link Evictable} entries
     * @param evictionPolicyEvaluator   {@link EvictionPolicyEvaluator} to evaluate
     *
     * @return evicted entry count
     */
    @Override
    protected int evictInternal(S sampleableEvictableStore,
            EvictionPolicyEvaluator<A, E> evictionPolicyEvaluator) {
        final Iterable<EvictionCandidate<A, E>> samples =
                sampleableEvictableStore.sample(SAMPLE_COUNT);
        final Iterable<EvictionCandidate<A, E>> evictionCandidates =
                evictionPolicyEvaluator.evaluate(samples);
        return sampleableEvictableStore.evict(evictionCandidates);
    }

}
