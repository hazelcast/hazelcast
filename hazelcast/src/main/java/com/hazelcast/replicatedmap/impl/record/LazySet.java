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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

class LazySet<K, V, R> implements Set<R> {

    private final InternalReplicatedMapStorage<K, V> storage;
    private final IteratorFactory<K, V, R> iteratorFactory;

    LazySet(IteratorFactory<K, V, R> iteratorFactory, InternalReplicatedMapStorage<K, V> storage) {
        this.iteratorFactory = iteratorFactory;
        this.storage = storage;
    }

    @Override
    public int size() {
        return storage.size();
    }

    @Override
    public boolean isEmpty() {
        return storage.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException("LazySet does not support contains requests");
    }

    @Override
    public Iterator<R> iterator() {
        Iterator<Map.Entry<K, ReplicatedRecord<K, V>>> iterator = storage.entrySet().iterator();
        return iteratorFactory.create(iterator);
    }

    @Override
    public Object[] toArray() {
        List<Object> result = new ArrayList<>(storage.values().size());
        for (R r : this) {
            // we cannot use addAll() here, since it results in StackOverflowError
            //noinspection UseBulkOperation
            result.add(r);
        }
        return result.toArray(new Object[0]);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        List<Object> result = new ArrayList<>(storage.values().size());
        for (R r : this) {
            // we cannot use addAll() here, since it results in StackOverflowError
            //noinspection UseBulkOperation
            result.add(r);
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
    public boolean add(R e) {
        throw new UnsupportedOperationException("LazySet is not modifiable");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("LazySet is not modifiable");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException("LazySet does not support contains requests");
    }

    @Override
    public boolean addAll(Collection<? extends R> c) {
        throw new UnsupportedOperationException("LazySet is not modifiable");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("LazySet is not modifiable");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("LazySet is not modifiable");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("LazySet is not modifiable");
    }

    public interface IteratorFactory<K, V, R> {
        Iterator<R> create(Iterator<Map.Entry<K, ReplicatedRecord<K, V>>> iterator);
    }
}
