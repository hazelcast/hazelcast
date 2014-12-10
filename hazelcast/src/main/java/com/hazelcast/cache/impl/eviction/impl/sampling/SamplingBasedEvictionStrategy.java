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

import com.hazelcast.cache.impl.eviction.AbstractEvictionStrategy;
import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;
import com.hazelcast.cache.impl.eviction.EvictionEvaluator;
import com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy;
import com.hazelcast.cache.impl.eviction.EvictionStrategy;

/**
 * Sampling based {@link EvictionStrategy} implementation.
 * This strategy selects 15 random {@link Evictable} samples (entries9 from given {@link SampleableEvictableStore}
 * instance and passes them to the {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy} to find the
 * best matching candidate for eviction.
 *
 * @param <A> accessor (key) type of the evictable entry
 * @param <E> {@link com.hazelcast.cache.impl.eviction.Evictable} type (value) of the entry
 * @param <S> type of the {@link com.hazelcast.cache.impl.eviction.EvictableStore} that stores the entries
 */
public class SamplingBasedEvictionStrategy<A, E extends Evictable, S extends SampleableEvictableStore<A, E>>
        extends AbstractEvictionStrategy<A, E, S> {

    private static final int DEFAULT_SAMPLING_COUNT = 15;

    /**
     * Processes sampling based eviction logic on {@link SampleableEvictableStore}.
     *
     * @param evictableStore         {@link SampleableEvictableStore} that holds {@link Evictable} entries
     * @param evictionPolicyStrategy {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy} to evaluate
     * @return The count of evicted entries
     */
    @Override
    public int evict(S evictableStore, EvictionPolicyStrategy<A, E> evictionPolicyStrategy,
                     EvictionEvaluator<A, E, S> evictionEvaluator) {

        if (evictableStore == null) {
            return 0;
        }

        if (evictionEvaluator != null && !evictionEvaluator.isEvictionRequired(evictableStore)) {
            return 0;
        }

        Iterable<EvictionCandidate<A, E>> samples = evictableStore.sample(DEFAULT_SAMPLING_COUNT);
        try {
            Iterable<EvictionCandidate<A, E>> evictionCandidates = evictionPolicyStrategy.evaluate(samples);
            return evictableStore.evict(evictionCandidates);
        } finally {
            evictableStore.cleanupSampling(samples);
        }
    }

}
