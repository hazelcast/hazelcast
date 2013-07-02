/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.transaction.TransactionalObject;

import java.util.Collection;

/**
 *
 * Transactional implementation of {@link BaseMultiMap}.
 *
 * @see BaseMultiMap
 * @see MultiMap
 * @param <K> key
 * @param <V> value
 */
public interface TransactionalMultiMap<K, V> extends BaseMultiMap<K, V>, TransactionalObject {

    /**
     * {@inheritDoc}
     */
    boolean put(K key, V value);

    /**
     * {@inheritDoc}
     */
    Collection<V> get(K key);

    /**
     * {@inheritDoc}
     */
    boolean remove(Object key, Object value);

    /**
     * {@inheritDoc}
     */
    Collection<V> remove(Object key);

    /**
     * {@inheritDoc}
     */
    int valueCount(K key);

    /**
     * {@inheritDoc}
     */
    int size();

}
