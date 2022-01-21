/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.connector;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class ArrayMap<K, V> extends AbstractMap<K, V> {

    private final List<Entry<K, V>> entries;
    private final ArrayMap.ArraySet set = new ArrayMap.ArraySet();

    ArrayMap() {
        entries = new ArrayList<>();
    }

    ArrayMap(int size) {
        entries = new ArrayList<>(size);
    }

    @Override @Nonnull
    public Set<Entry<K, V>> entrySet() {
        return set;
    }

    public void add(Map.Entry<K, V> entry) {
        entries.add(entry);
    }

    @Override
    public V get(Object key) {
        throw new UnsupportedOperationException();
    }

    private class ArraySet extends AbstractSet<Entry<K, V>> {

        @Override @Nonnull
        public Iterator<Entry<K, V>> iterator() {
            return entries.iterator();
        }

        @Override
        public int size() {
            return entries.size();
        }

        @Override
        public void clear() {
            entries.clear();
        }
    }

    @Override
    public String toString() {
        return entries.toString();
    }
}
