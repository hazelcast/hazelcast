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

package com.hazelcast.aggregation.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.Comparables;
import com.hazelcast.query.impl.Numbers;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public final class CanonicalizingHashSet<E> implements Set<E>, IdentifiedDataSerializable {

    private HashMap<Object, E> map;

    private Numbers.TypeInferrer typeInferrer = new Numbers.TypeInferrer();

    public CanonicalizingHashSet() {
        this.map = new HashMap<Object, E>();
    }

    public CanonicalizingHashSet(int capacity) {
        this.map = new HashMap<Object, E>(capacity);
    }

    public void addAll(CanonicalizingHashSet<E> set) {
        map.putAll(set.map);
        typeInferrer.observe(set.typeInferrer);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(typeInferrer);
        out.writeInt(size());
        for (Object element : this) {
            out.writeObject(element);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        typeInferrer = in.readObject();
        int count = in.readInt();
        this.map = new HashMap<Object, E>(count);
        for (int i = 0; i < count; i++) {
            E element = in.readObject();
            add(element);
        }
    }

    @Override
    public int getFactoryId() {
        return AggregatorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return AggregatorDataSerializerHook.CANONICALIZING_SET;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return map.containsKey(canonicalize(o));
    }

    @Override
    public Iterator<E> iterator() {
        // FIXME type inferrer support
        return map.values().iterator();
    }

    @Override
    public Object[] toArray() {
        // FIXME type inferrer support
        return map.values().toArray();
    }

    @SuppressWarnings({"NullableProblems", "SuspiciousToArrayCall"})
    @Override
    public <T> T[] toArray(T[] a) {
        // FIXME type inferrer support
        return map.values().toArray(a);
    }

    @Override
    public boolean add(E e) {
        return map.put(canonicalize(e), e) == null;
    }

    @Override
    public boolean remove(Object o) {
        return map.remove(canonicalize(o)) != null;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object element : c) {
            if (!contains(element)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean changed = false;
        for (E element : c) {
            if (add(element)) {
                changed = true;
            }
        }
        return changed;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public boolean retainAll(Collection<?> c) {
        // no efficient way of implementing, better just to avoid this method
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean changed = false;
        for (Object element : c) {
            if (remove(element)) {
                changed = true;
            }
        }
        return changed;
    }

    @Override
    public void clear() {
        map.clear();
        typeInferrer.reset();
    }

    @Override
    public int hashCode() {
        return map.keySet().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof Set)) {
            return false;
        }
        Set that = (Set) obj;

        return containsAll(that);
    }

    @Override
    public String toString() {
        // FIXME type inferrer support
        return map.values().toString();
    }

    private static Object canonicalize(Object value) {
        if (value instanceof Comparable) {
            return Comparables.canonicalizeForHashLookup((Comparable) value);
        }

        return value;
    }

}
