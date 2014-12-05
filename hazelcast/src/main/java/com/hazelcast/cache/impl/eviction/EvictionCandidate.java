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
 * The {@link com.hazelcast.cache.impl.eviction.EvictionCandidate} interface defines elements (such as records)
 * that can be evictable compared using {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}
 * implementations and provide access to an accessor (e.g. key or id) via its accessor method.
 *
 * @param <A> Type of the accessor
 * @param <E> Type of the {@link Evictable} value
 */
public interface EvictionCandidate<A, E extends Evictable> {

    /**
     * The accessor is any kind of access key to make it able for an {@link EvictableStore} to
     * remove a {@link EvictionCandidate} from the internal storage data structure
     *
     * @return The accessor (key or id) to remove the entry from the
     * {@link com.hazelcast.cache.impl.eviction.EvictableStore} corresponding implementation
     */
    A getAccessor();

    /**
     * The returned {@link Evictable} is the actual candidate of eviction to decide on to be
     * evicted or not
     *
     * @return The evictable this candidate represents
     */
    E getEvictable();
}
