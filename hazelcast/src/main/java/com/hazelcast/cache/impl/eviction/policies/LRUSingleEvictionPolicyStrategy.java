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

package com.hazelcast.cache.impl.eviction.policies;

import com.hazelcast.cache.impl.eviction.Evictable;

/**
 * This class is a LRU (Less Recently Used) implementation of the
 * {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}
 *
 * @param <A> accessor (key) type of the evictable entry
 * @param <E> {@link com.hazelcast.cache.impl.eviction.Evictable} type (value) of the entry
 */
public class LRUSingleEvictionPolicyStrategy<A, E extends Evictable>
        extends AbstractSingleEvictionPolicyStrategy<A, E> {

    @Override
    protected Evictable compareEvictable(Evictable current, Evictable candidate) {
        return current.getLastAccessTime() > candidate.getLastAccessTime() ? candidate : current;
    }
}
