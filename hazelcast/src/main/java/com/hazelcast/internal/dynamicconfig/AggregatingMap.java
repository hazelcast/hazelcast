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

package com.hazelcast.internal.dynamicconfig;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;

/**
 * Provides unmodifiable view aggregating 2 maps with distinct key sets.
 * When keys exist in both maps then behaviour is undefined.
 *
 * @param <K>
 * @param <V>
 */
public final class AggregatingMap<K, V> implements Map<K, V> {
    private final Map<K, V> map1;
    private final Map<K, V> map2;

    private AggregatingMap(Map<K, V> map1, Map<K, V> map2) {
        if (map1 == null) {
            map1 = Collections.emptyMap();
        }
        if (map2 == null) {
            map2 = Collections.emptyMap();
        }
        this.map1 = map1;
        this.map2 = map2;
    }

    /**
     * Creates new aggregating maps.
     *
     * @param map1
     * @param map2
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> Map<K, V> aggregate(Map<K, V> map1, Map<K, V> map2) {
        return new AggregatingMap<K, V>(map1, map2);
    }

    @Override
    public int size() {
        return map1.size() + map2.size();
    }

    @Override
    public boolean isEmpty() {
        return map1.isEmpty() && map2.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map1.containsKey(key) || map2.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map1.containsValue(value) || map2.containsValue(value);
    }

    @Override
    public V get(Object key) {
        V v = map1.get(key);
        return v == null ? map2.get(key) : v;
    }

    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException("aggregating map is read only");
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException("aggregating map is read only");
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("aggregating map is read only");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("aggregating map is read only");
    }

    @Override
    public Set<K> keySet() {
        HashSet<K> keys = new HashSet<K>(map1.keySet());
        keys.addAll(map2.keySet());
        return unmodifiableSet(keys);
    }

    @Override
    public Collection<V> values() {
        ArrayList<V> values = new ArrayList<V>(map1.values());
        values.addAll(map2.values());
        return unmodifiableCollection(values);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        Set<Entry<K, V>> entrySet1 = map1.entrySet();
        Set<Entry<K, V>> entrySet2 = map2.entrySet();

        HashSet<Entry<K, V>> aggregatedEntrySet = new HashSet<Entry<K, V>>();
        copyEntries(entrySet1, aggregatedEntrySet);
        copyEntries(entrySet2, aggregatedEntrySet);
        return unmodifiableSet(aggregatedEntrySet);
    }

    private void copyEntries(Set<Entry<K, V>> source, Set<Entry<K, V>> destination) {
        for (Entry<K, V> entry : source) {
            K key = entry.getKey();
            V value = entry.getValue();
            destination.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
    }
}
