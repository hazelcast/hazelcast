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
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.impl.evaluator.EvictionPolicyEvaluator;

/**
 * This strategy samples {@link Evictable} entries from {@link SampleableEvictableStore}, orders candidates
 * for eviction according to the provided EvictionPolicyEvaluator.
 */
public final class SamplingEvictionStrategy<A, E extends Evictable, S extends SampleableEvictableStore<A, E>> {

    public static final SamplingEvictionStrategy INSTANCE = new SamplingEvictionStrategy();

    private static final int SAMPLE_COUNT = 15;

    private SamplingEvictionStrategy() {
    }

    /**
     * Does eviction if required.
     *
     * @param evictableStore            Store that holds {@link Evictable} entries
     * @param evictionPolicyEvaluator   {@link EvictionPolicyEvaluator} to evaluate
     *                                  {@link com.hazelcast.config.EvictionPolicy} on entries
     * @param evictionChecker            {@link EvictionChecker} to check whether max size is reached, therefore
     *                                  eviction is required or not.
     * @param evictionListener          {@link EvictionListener} to listen evicted entries
     *
     * @return true is an entry was evicted, otherwise false
     */
    public boolean evict(S evictableStore, EvictionPolicyEvaluator<A, E> evictionPolicyEvaluator,
                     EvictionChecker evictionChecker, EvictionListener<A, E> evictionListener) {
        if (evictionChecker != null) {
            if (evictionChecker.isEvictionRequired()) {
                return evictInternal(evictableStore, evictionPolicyEvaluator, evictionListener);
            } else {
                return false;
            }
        } else {
            return evictInternal(evictableStore, evictionPolicyEvaluator, evictionListener);
        }
    }

    /**
     * Processes sampling based eviction logic on {@link SampleableEvictableStore}.
     *
     * @param sampleableEvictableStore  {@link SampleableEvictableStore} that holds {@link Evictable} entries
     * @param evictionPolicyEvaluator   {@link EvictionPolicyEvaluator} to evaluate
     * @param evictionListener          {@link EvictionListener} to listen evicted entries
     *
     * @return true is an entry was evicted, otherwise false
     */
    protected boolean evictInternal(S sampleableEvictableStore,
            EvictionPolicyEvaluator<A, E> evictionPolicyEvaluator,
            EvictionListener<A, E> evictionListener) {
        final Iterable<EvictionCandidate<A, E>> samples = sampleableEvictableStore.sample(SAMPLE_COUNT);
        final EvictionCandidate<A, E> evictionCandidate = evictionPolicyEvaluator.evaluate(samples);
        return sampleableEvictableStore.tryEvict(evictionCandidate, evictionListener);
    }

}
