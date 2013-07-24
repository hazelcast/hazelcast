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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Adapter for MapStore.
 *
 * @param <K> key of the map entry
 * @param <V> value of the map entry.
 * @see com.hazelcast.core.MapStore
 */
public class MapStoreAdapter<K, V> implements MapStore<K, V> {

    /**
     * {@inheritDoc}
     */
    public void delete(final K key) {
    }

    /**
     * {@inheritDoc}
     */
    public void store(final K key, final V value) {
    }

    /**
     * {@inheritDoc}
     */
    public void storeAll(final Map<K, V> map) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }
    }

    /**
     * {@inheritDoc}
     */
    public void deleteAll(final Collection<K> keys) {
        for (K key : keys) {
            delete(key);
        }
    }

    /**
     * {@inheritDoc}
     */
    public V load(final K key) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    public Map<K, V> loadAll(final Collection<K> keys) {
        Map<K, V> result = new HashMap<K, V>();
        for (K key : keys) {
            V value = load(key);
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    public Set<K> loadAllKeys() {
        return null;
    }
}
