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
 * An abstract base class for stateless eviction implementations to evict {@link EvictableStore} implementations
 * as specified {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}. This class implements the hook
 * methods in a delegate-only way.
 *
 * @param <A> accessor (key) type of the evictable entry
 * @param <E> {@link com.hazelcast.cache.impl.eviction.Evictable} type (value) of the entry
 * @param <S> type of the {@link com.hazelcast.cache.impl.eviction.EvictableStore} that stores the entries
 */
public abstract class AbstractEvictionStrategy<A, E extends Evictable, S extends EvictableStore<A, E>>
        implements EvictionStrategy<A, E, S> {

    @Override
    public void onCreation(S evictableStore, EvictionPolicyStrategy evictionPolicyStrategy, Evictable evictable) {
        evictionPolicyStrategy.onCreation(evictable);
    }

    @Override
    public void onLoad(S evictableStore, EvictionPolicyStrategy evictionPolicyStrategy, Evictable evictable) {
        evictionPolicyStrategy.onLoad(evictable);
    }

    @Override
    public void onRead(S evictableStore, EvictionPolicyStrategy evictionPolicyStrategy, Evictable evictable) {
        evictionPolicyStrategy.onRead(evictable);
    }

    @Override
    public void onUpdate(S evictableStore, EvictionPolicyStrategy evictionPolicyStrategy, Evictable evictable) {
        evictionPolicyStrategy.onUpdate(evictable);
    }

    @Override
    public void onRemove(S evictableStore, EvictionPolicyStrategy evictionPolicyStrategy, Evictable evictable) {
        evictionPolicyStrategy.onRemove(evictable);
    }
}
