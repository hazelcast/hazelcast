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

package com.hazelcast.cache;

import com.hazelcast.spi.eviction.EvictionPolicyComparator;

/**
 * {@link ICache} specific {@link EvictionPolicyComparator}
 * for comparing {@link CacheEntryView}s to be evicted.
 *
 * Implementors of the comparator have to implement {@code equals} and {@code hashCode} methods
 * to support correct config comparison.
 *
 * @param <K> type of the key
 * @param <V> type of the value
 * @see EvictionPolicyComparator
 * @see CacheEntryView
 */
@FunctionalInterface
public interface CacheEvictionPolicyComparator<K, V>
        extends EvictionPolicyComparator<K, V, CacheEntryView<K, V>> {

    /**
     * Compares the given {@link CacheEntryView} instances and
     * returns the result. The result should be one of
     * <ul>
     *   <li>-1: first entry has higher priority to be evicted</li>
     *   <li> 1: second entry has higher priority to be evicted</li>
     *   <li> 0: both entries have same priority</li>
     * </ul>
     *
     * @param o1 the first {@link CacheEntryView} instance to be compared
     * @param o2 the second {@link CacheEntryView} instance to be compared
     * @return the result of comparison
     */
    @Override
    int compare(CacheEntryView<K, V> o1, CacheEntryView<K, V> o2);
}
