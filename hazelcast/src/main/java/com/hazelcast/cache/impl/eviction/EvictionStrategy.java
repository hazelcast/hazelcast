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
 * Interface for eviction implementations to evict {@link EvictableStore} implementations as specified
 * {@link EvictionPolicyEvaluator}.
 */
public interface EvictionStrategy<A, E extends Evictable, S extends EvictableStore<A, E>> {

    /**
     * Does eviction if eviction is required by given {@link EvictionChecker}.
     *
     * @param evictableStore            Store that holds {@link Evictable} entries
     * @param evictionPolicyEvaluator   {@link EvictionPolicyEvaluator} to evaluate
     *                                  {@link com.hazelcast.config.EvictionPolicy} on entries
     * @param evictionChecker           {@link EvictionChecker} to make a decision about if eviction is
     *                                  required or not. If you want evict anyway,
     *                                  you can use {@link EvictionChecker#EVICT_ALWAYS}
     *
     * @return evicted entry count
     */
    int evict(S evictableStore, EvictionPolicyEvaluator<A, E> evictionPolicyEvaluator,
            EvictionChecker evictionChecker);

}
