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

package com.hazelcast.transaction;

import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.multimap.BaseMultiMap;
import com.hazelcast.multimap.MultiMap;

import java.util.Collection;

/**
 * Transactional implementation of {@link BaseMultiMap}.
 * <p>
 * Supports split brain protection {@link SplitBrainProtectionConfig} since 3.10 in
 * cluster versions 3.10 and higher.
 *
 * @param <K> type of the multimap key
 * @param <V> type of the multimap value
 * @see BaseMultiMap
 * @see MultiMap
 */
public interface TransactionalMultiMap<K, V> extends BaseMultiMap<K, V>, TransactionalObject {

    /**
     * {@inheritDoc}
     */
    @Override
    boolean put(K key, V value);

    /**
     * {@inheritDoc}
     */
    @Override
    Collection<V> get(K key);

    /**
     * {@inheritDoc}
     */
    @Override
    boolean remove(Object key, Object value);

    /**
     * {@inheritDoc}
     */
    @Override
    Collection<V> remove(Object key);

    /**
     * {@inheritDoc}
     */
    @Override
    int valueCount(K key);

    /**
     * {@inheritDoc}
     */
    @Override
    int size();
}
