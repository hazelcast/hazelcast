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

package com.hazelcast.map.eviction;

import com.hazelcast.core.EntryView;

import java.util.Comparator;

/**
 * An eviction policy takes its role after the decision that a map needs to free some memory
 * according to one of {@link com.hazelcast.config.MaxSizeConfig.MaxSizePolicy MaxSizePolicies}
 * and it helps to find most appropriate entries to remove.
 *
 * {@link com.hazelcast.core.IMap IMap} has out-of-the-box eviction policies like {@link LRUEvictionPolicy},
 * {@link LFUEvictionPolicy} and {@link RandomEvictionPolicy} but if there is a need to define another eviction policy,
 * users can introduce a custom one by extending {@link MapEvictionPolicy} class.
 *
 * <p>Implementation:</p>
 * {@link MapEvictionPolicy#compare(EntryView, EntryView)} method must be implemented to provide an ascending order
 * of entries. Because internal eviction algorithm will evict the smallest entry from {@link com.hazelcast.core.IMap IMap}
 *
 * @param <K> the type of keys maintained by IMap
 * @param <V> the type of mapped values
 *
 * @see LRUEvictionPolicy
 * @see LFUEvictionPolicy
 * @see RandomEvictionPolicy
 *
 * @since 3.7
 */
public abstract class MapEvictionPolicy<K, V> implements Comparator<EntryView<K, V>> {

    /**
     * {@inheritDoc}
     *
     * <p>
     * If you prefer to evict the 1st entry then return negative number, if the 2nd entry is a better candidate
     * then return a positive number or return 0 if both entries are equally good candidates for eviction
     * </p>
     */
    @Override
    public abstract int compare(EntryView<K, V> entryView1, EntryView<K, V> entryView2);
}
