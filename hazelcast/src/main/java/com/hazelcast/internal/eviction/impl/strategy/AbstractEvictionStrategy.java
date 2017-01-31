/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.eviction.impl.strategy;

import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.EvictableStore;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.EvictionPolicyEvaluator;
import com.hazelcast.internal.eviction.EvictionStrategy;

/**
 * Base class for eviction implementations to evict {@link EvictableStore} implementations as specified
 * {@link EvictionPolicyEvaluator}.
 */
public abstract class AbstractEvictionStrategy<A, E extends Evictable, S extends EvictableStore<A, E>>
        implements EvictionStrategy<A, E, S> {

    /**
     * Does eviction if eviction is required by given {@link EvictionChecker}.
     *
     * @param evictableStore            Store that holds {@link Evictable} entries
     * @param evictionPolicyEvaluator   {@link EvictionPolicyEvaluator} to evaluate
     *                                  {@link com.hazelcast.config.EvictionPolicy} on entries
     * @param evictionChecker           {@link EvictionChecker} to make a decision about if eviction is
     *                                  required or not. If you want evict anyway,
     *                                  you can use {@link EvictionChecker#EVICT_ALWAYS}
     * @param evictionListener          {@link EvictionListener} to listen evicted entries
     *
     * @return evicted entry count
     */
    @Override
    public int evict(S evictableStore, EvictionPolicyEvaluator<A, E> evictionPolicyEvaluator,
            EvictionChecker evictionChecker, EvictionListener<A, E> evictionListener) {
        if (evictionChecker != null) {
            if (evictionChecker.isEvictionRequired()) {
                return evictInternal(evictableStore, evictionPolicyEvaluator, evictionListener);
            } else {
                return 0;
            }
        } else {
            return evictInternal(evictableStore, evictionPolicyEvaluator, evictionListener);
        }
    }

    /**
     * Does eviction internally.
     *
     * @param evictableStore            Store that holds {@link Evictable} entries
     * @param evictionPolicyEvaluator   {@link EvictionPolicyEvaluator} to evaluate
     *                                  {@link com.hazelcast.config.EvictionPolicy} on entries
     * @param evictionListener          {@link EvictionListener} to listen evicted entries
     *
     * @return evicted entry count
     */
    protected abstract int evictInternal(S evictableStore,
            EvictionPolicyEvaluator<A, E> evictionPolicyEvaluator,
            EvictionListener<A, E> evictionListener);

}
