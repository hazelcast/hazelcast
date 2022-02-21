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

package com.hazelcast.internal.util.collection;

import com.hazelcast.multimap.MultiMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Simplistic implementation of MultiMap.
 * It's not thread-safe, concurrent access has to be externally synchronized.
 *
 * It does not allow duplicates: The same value can be associated with the same key once only. Duplicates are
 * eliminated.
 *
 * The name has a prefix Internal- to avoid confusion with {@link MultiMap}
 *
 * @param <K>
 * @param <V>
 */
public class InternalSetMultimap<K, V> {
    private final Map<K, Set<V>> backingMap;

    public InternalSetMultimap() {
        this.backingMap = new HashMap<K, Set<V>>();
    }

    /**
     * Associate value to a given key. It has no effect if the value is already associated with the key.
     *
     * @param key
     * @param value
     */
    public void put(K key, V value) {
        checkNotNull(key, "Key cannot be null");
        checkNotNull(value, "Value cannot be null");

        Set<V> values = backingMap.get(key);
        if (values == null) {
            values = new HashSet<V>();
            backingMap.put(key, values);
        }
        values.add(value);
    }

    /**
     * Return Set of values associated with a given key
     *
     * @param key
     * @return
     */
    public Set<V> get(K key) {
        checkNotNull(key, "Key cannot be null");
        return backingMap.get(key);
    }

    public Set<Map.Entry<K, Set<V>>> entrySet() {
        return backingMap.entrySet();
    }
}
