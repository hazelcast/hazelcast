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
import java.util.Properties;

/**
 * This interface defines an eviction policy to implement the actual implementation of the eviction
 * selection algorithm, such as LRU or LFU.<br>
 * Implementations can have local state and use the hook methods (such as {@link #onCreation(Evictable)})
 * to update the internal state based on changes happen in the storage.
 *
 * @param <A> accessor (key) type of the evictable entry
 * @param <E> {@link com.hazelcast.cache.impl.eviction.Evictable} type (value) of the entry
 */
public interface EvictionPolicyStrategy<A, E extends Evictable>
        extends Serializable {

    /**
     * The apply method implements the actual policy rules and is called on every eviction to select one or
     * more candidates to be evicted from the given input set of {@link EvictionCandidate}s.<br/>
     * The selection algorithm should execute in a constant time to deliver a predictable timing results of
     * the eviction system.
     *
     * @param evictionCandidates Multiple {@link EvictionCandidate}s to select one or more candidates to be
     *                           evicted
     * @return One or more selected {@link EvictionCandidate} entries to evict
     */
    Iterable<EvictionCandidate<A, E>> evaluate(Iterable<EvictionCandidate<A, E>> evictionCandidates);

    /**
     * A hook method that is called when new entries are created on the passed {@link EvictableStore} instance.<br>
     * This method is meant to be used to update internal state of {@link com.hazelcast.cache.impl.eviction.EvictionStrategy}
     * and also has to delegate the call to the given {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}.
     *
     * @param evictable The created evictable
     */
    void onCreation(Evictable evictable);

    /**
     * A hook method that is called when an entry is loaded on the passed {@link EvictableStore} instance by an configured
     * external resource like a database storage or similar.<br>
     * This method is meant to be used to update internal state of {@link com.hazelcast.cache.impl.eviction.EvictionStrategy}
     * and also has to delegate the call to the given {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}.
     *
     * @param evictable The loaded evictable
     */
    void onLoad(Evictable evictable);

    /**
     * A hook method that is called when entries are accessed (get) in the passed {@link EvictableStore} instance.<br>
     * This method is meant to be used to update internal state of {@link com.hazelcast.cache.impl.eviction.EvictionStrategy}
     * and also has to delegate the call to the given {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}.
     *
     * @param evictable The accessed evictable
     */
    void onRead(Evictable evictable);

    /**
     * A hook method that is called when entries are updated in the passed {@link EvictableStore} instance.<br>
     * This method is meant to be used to update internal state of {@link com.hazelcast.cache.impl.eviction.EvictionStrategy}
     * and also has to delegate the call to the given {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}.
     *
     * @param evictable The updated evictable
     */
    void onUpdate(Evictable evictable);

    /**
     * A hook method that is called when entries are removed in the passed {@link EvictableStore} instance.<br>
     * This method is meant to be used to update or cleanup internal state of
     * {@link com.hazelcast.cache.impl.eviction.EvictionStrategy} and also has to delegate the call to the given
     * {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}.
     *
     * @param evictable The removed evictable
     */
    void onRemove(Evictable evictable);

    /**
     * The configure method is called after constructing the instance to pass in properties defined by the
     * user. Configuration properties might be different from each other and are meant to be implementation
     * specific.
     *
     * @param properties The properties instance containing the configuration properties
     */
    void configure(Properties properties);
}
