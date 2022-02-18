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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.replicatedmap.impl.record.LazySet.IteratorFactory;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

class LazyCollection<K, V> implements Collection<V> {

    private final InternalReplicatedMapStorage<K, V> storage;
    private final IteratorFactory<K, V, V> iteratorFactory;
    private final Collection<ReplicatedRecord<K, V>> values;

    LazyCollection(IteratorFactory<K, V, V> iteratorFactory, InternalReplicatedMapStorage<K, V> storage) {
        this.iteratorFactory = iteratorFactory;
        this.values = storage.values();
        this.storage = storage;
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public boolean contains(Object value) {
        throw new UnsupportedOperationException("LazySet does not support contains requests");
    }

    @Override
    public Iterator<V> iterator() {
        Iterator<Entry<K, ReplicatedRecord<K, V>>> iterator = storage.entrySet().iterator();
        return iteratorFactory.create(iterator);
    }

    @Override
    public Object[] toArray() {
        List<Object> result = new ArrayList<>(storage.values().size());
        Iterator<V> iterator = iterator();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result.toArray(new Object[0]);
    }

    @Override
    public <T> T[] toArray(T[] a) {
        List<Object> result = new ArrayList<>(storage.values().size());
        Iterator<V> iterator = iterator();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        if (a.length != result.size()) {
            a = (T[]) Array.newInstance(a.getClass().getComponentType(), result.size());
        }
        for (int i = 0; i < a.length; i++) {
            a[i] = (T) result.get(i);
        }
        return a;
    }

    @Override
    public boolean add(V v) {
        throw new UnsupportedOperationException("LazyList is not modifiable");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("LazyList is not modifiable");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException("LazySet does not support contains requests");
    }

    @Override
    public boolean addAll(Collection<? extends V> c) {
        throw new UnsupportedOperationException("LazyList is not modifiable");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("LazyList is not modifiable");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("LazyList is not modifiable");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("LazyList is not modifiable");
    }
}
