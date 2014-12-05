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

import java.io.Serializable;

/**
 * This interface defines an eviction contract for different strategies to select candidate entries to evict
 * from {@link EvictableStore} implementations as specified by an {@link EvictionPolicyStrategy} policy
 * implementations.
 *
 * @param <A> accessor (key) type of the evictable entry
 * @param <E> {@link com.hazelcast.cache.impl.eviction.Evictable} type (value) of the entry
 * @param <S> type of the {@link com.hazelcast.cache.impl.eviction.EvictableStore} that stores the entries
 */
public interface EvictionStrategy<A, E extends Evictable, S extends EvictableStore<A, E>>
        extends Serializable {

    /**
     * The evict method is called by a storage to trigger eviction from the passed in {@link EvictableStore}.
     * It is up to the implementation to provide a full set of entries as candidates to the
     * {@link EvictionPolicyStrategy} or to select just a subset to speed up eviction.
     * The selection algorithm should execute in a constant time to deliver a predictable timing results of
     * the eviction system.<br>
     * The passed in {@link EvictionEvaluator} instance will be used to evaluate if an eviction is required
     * or not. Passing in <tt>null</tt> forces an eviction to happen.
     *
     * @param evictableStore         The storage that {@link com.hazelcast.cache.impl.eviction.Evictable}s are
     *                               meant to be take part in eviction
     * @param evictionPolicyStrategy An eviction policy to evaluate {@link com.hazelcast.cache.impl.eviction.EvictionCandidate}
     *                               with and find the best matching of the candidates to evict
     * @param evictionEvaluator      The evaluator to decide if eviction is required of not. If <tt>null</tt> is
     *                               passed as a parameter an eviction is enforced.
     * @return The count of evicted entries
     */
    int evict(S evictableStore, EvictionPolicyStrategy<A, E> evictionPolicyStrategy,
              EvictionEvaluator<A, E, S> evictionEvaluator);

    /**
     * A hook method that is called when new entries are created on the passed {@link EvictableStore} instance.<br>
     * This method is meant to be used to update internal state of {@link com.hazelcast.cache.impl.eviction.EvictionStrategy}
     * and also has to delegate the call to the given {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}.
     *
     * @param evictableStore         The origin storage of the event
     * @param evictionPolicyStrategy The eviction policy implementation for decision making based on the type and to delegate
     *                               the call to
     * @param evictable              The created evictable
     */
    void onCreation(S evictableStore, EvictionPolicyStrategy evictionPolicyStrategy, Evictable evictable);

    /**
     * A hook method that is called when an entry is loaded on the passed {@link EvictableStore} instance by an configured
     * external resource like a database storage or similar.<br>
     * This method is meant to be used to update internal state of {@link com.hazelcast.cache.impl.eviction.EvictionStrategy}
     * and also has to delegate the call to the given {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}.
     *
     * @param evictableStore         The origin storage of the event
     * @param evictionPolicyStrategy The eviction policy implementation for decision making based on the type and to delegate
     *                               the call to
     * @param evictable              The loaded evictable
     */
    void onLoad(S evictableStore, EvictionPolicyStrategy evictionPolicyStrategy, Evictable evictable);

    /**
     * A hook method that is called when entries are accessed (get) in the passed {@link EvictableStore} instance.<br>
     * This method is meant to be used to update internal state of {@link com.hazelcast.cache.impl.eviction.EvictionStrategy}
     * and also has to delegate the call to the given {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}.
     *
     * @param evictableStore         The origin storage of the event
     * @param evictionPolicyStrategy The eviction policy implementation for decision making based on the type and to delegate
     *                               the call to
     * @param evictable              The accessed evictable
     */
    void onRead(S evictableStore, EvictionPolicyStrategy evictionPolicyStrategy, Evictable evictable);

    /**
     * A hook method that is called when entries are updated in the passed {@link EvictableStore} instance.<br>
     * This method is meant to be used to update internal state of {@link com.hazelcast.cache.impl.eviction.EvictionStrategy}
     * and also has to delegate the call to the given {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}.
     *
     * @param evictableStore         The origin storage of the event
     * @param evictionPolicyStrategy The eviction policy implementation for decision making based on the type and to delegate
     *                               the call to
     * @param evictable              The updated evictable
     */
    void onUpdate(S evictableStore, EvictionPolicyStrategy evictionPolicyStrategy, Evictable evictable);

    /**
     * A hook method that is called when entries are removed in the passed {@link EvictableStore} instance.<br>
     * This method is meant to be used to update or cleanup internal state of
     * {@link com.hazelcast.cache.impl.eviction.EvictionStrategy} and also has to delegate the call to the given
     * {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}.
     *
     * @param evictableStore         The origin storage of the event
     * @param evictionPolicyStrategy The eviction policy implementation for decision making based on the type and to delegate
     *                               the call to
     * @param evictable              The removed evictable
     */
    void onRemove(S evictableStore, EvictionPolicyStrategy evictionPolicyStrategy, Evictable evictable);
}
