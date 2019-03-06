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

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.Comparables;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public final class DistinctValuesAggregator<I, R> extends AbstractAggregator<I, R, Set<R>> implements IdentifiedDataSerializable {

    Map<Object, R> values = new HashMap<Object, R>();

    public DistinctValuesAggregator() {
        super();
    }

    public DistinctValuesAggregator(String attributePath) {
        super(attributePath);
    }

    @Override
    public void accumulateExtracted(I entry, R value) {
        values.put(canonicalize(value), value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void combine(Aggregator aggregator) {
        DistinctValuesAggregator distinctValuesAggregator = (DistinctValuesAggregator) aggregator;
        this.values.putAll(distinctValuesAggregator.values);
    }

    @Override
    public Set<R> aggregate() {
        return new SetView<R>(values);
    }

    @Override
    public int getFactoryId() {
        return AggregatorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return AggregatorDataSerializerHook.DISTINCT_VALUES;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attributePath);
        out.writeInt(values.size());
        for (Object value : values.values()) {
            out.writeObject(value);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.attributePath = in.readUTF();
        int count = in.readInt();
        this.values = new HashMap<Object, R>(count);
        for (int i = 0; i < count; i++) {
            R value = in.readObject();
            values.put(canonicalize(value), value);
        }
    }

    private static Object canonicalize(Object value) {
        if (value instanceof Comparable) {
            return Comparables.canonicalizeForHashLookup((Comparable) value);
        }

        return value;
    }

    @SuppressWarnings("NullableProblems")
    private static class SetView<R> implements Set<R> {

        private final Map<Object, R> map;

        public SetView(Map<Object, R> map) {
            this.map = map;
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
        public Iterator<R> iterator() {
            return map.values().iterator();
        }

        @Override
        public Object[] toArray() {
            return map.values().toArray();
        }

        @SuppressWarnings("SuspiciousToArrayCall")
        @Override
        public <T> T[] toArray(T[] a) {
            return map.values().toArray(a);
        }

        @Override
        public boolean add(R r) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            for (Object value : c) {
                if (!contains(value)) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public boolean addAll(Collection<? extends R> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

    }

}
