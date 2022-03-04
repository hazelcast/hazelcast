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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Simplistic implementation of MultiMap.
 * It's not thread-safe, concurrent access has to be externally synchronized.
 *
 * It allows duplicates: The same value can be associated with the same key multiple times
 *
 * The name has a prefix Internal- to avoid confusion with {@link MultiMap}
 *
 * @param <K>
 * @param <V>
 */
public class InternalListMultiMap<K, V> {
    private final Map<K, List<V>> backingMap;

    public InternalListMultiMap() {
        this.backingMap = new HashMap<K, List<V>>();
    }

    /**
     * Put value to a given key. It allows duplicates under the same key
     *
     * @param key
     * @param value
     */
    public void put(K key, V value) {
        List<V> values = backingMap.get(key);
        if (values == null) {
            values = new ArrayList<V>();
            backingMap.put(key, values);
        }
        values.add(value);
    }

    /**
     * Return collection of values associated with a given key
     *
     * @param key
     * @return
     */
    public Collection<V> get(K key) {
        return backingMap.get(key);
    }

    public Set<Map.Entry<K, List<V>>> entrySet() {
        return backingMap.entrySet();
    }
}
