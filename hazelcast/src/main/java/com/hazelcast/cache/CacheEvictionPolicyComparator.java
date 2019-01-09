/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.internal.eviction.EvictionPolicyComparator;

/**
 * Cache specific {@link EvictionPolicyComparator} for comparing
 * {@link CacheEntryView}s to be evicted.
 *
 * @param <K> type of the key
 * @param <V> type of the value
 *
 * @see EvictionPolicyComparator
 * @see CacheEntryView
 */
public abstract class CacheEvictionPolicyComparator<K, V>
        extends EvictionPolicyComparator<K, V, CacheEntryView<K, V>> {

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract int compare(CacheEntryView<K, V> e1, CacheEntryView<K, V> e2);

}
