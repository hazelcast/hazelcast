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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.UnmodifiableListIterator;

import java.io.IOException;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.function.Predicate;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

/**
 * This is an unmodifiable lazy list which is not parametrized (no generic. needed for Jackson serializer).
 */
public class UnmodifiableLazyList extends AbstractList implements IdentifiedDataSerializable {

    private final transient SerializationService serializationService;
    private List list;

    public UnmodifiableLazyList() {
        this.serializationService = null;
    }

    public UnmodifiableLazyList(List list, SerializationService serializationService) {
        this.list = list;
        this.serializationService = serializationService;
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean add(Object t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(Predicate filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection coll) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(int index) {
        Object o = list.get(index);
        if (o instanceof Data) {
            Object item = serializationService.toObject(o);
            try {
                list.set(index, item);
            } catch (Exception e) {
                ignore(e);
            }
            return item;
        }
        return o;
    }

    @Override
    public Iterator iterator() {
        return listIterator(0);
    }

    @Override
    public ListIterator listIterator() {
        return listIterator(0);
    }

    @Override
    public ListIterator listIterator(int index) {
        return new UnmodifiableLazyListIterator(list.listIterator(index));
    }

    @Override
    public List subList(int fromIndex, int toIndex) {
        return new UnmodifiableLazyList(list.subList(fromIndex, toIndex), serializationService);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(list.size());
        for (Object o : this) {
            out.writeObject(o);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        list = new ArrayList(size);
        for (int i = 0; i < size; i++) {
            list.add(in.readObject());
        }
    }

    private class UnmodifiableLazyListIterator extends UnmodifiableListIterator {

        ListIterator listIterator;

        UnmodifiableLazyListIterator(ListIterator listIterator) {
            this.listIterator = listIterator;
        }

        @Override
        public boolean hasNext() {
            return listIterator.hasNext();
        }

        @Override
        public Object next() {
            return deserializeAndSet(listIterator.next());
        }

        @Override
        public boolean hasPrevious() {
            return listIterator.hasPrevious();
        }

        @Override
        public Object previous() {
            return deserializeAndSet(listIterator.previous());
        }

        @Override
        public int nextIndex() {
            return listIterator.nextIndex();
        }

        @Override
        public int previousIndex() {
            return listIterator.previousIndex();
        }

        private Object deserializeAndSet(Object o) {
            if (o instanceof Data) {
                Object item = serializationService.toObject(o);
                try {
                    listIterator.set(item);
                } catch (Exception e) {
                    ignore(e);
                }
                return item;
            } else if (o instanceof Map.Entry) {
                Map.Entry entry = (Map.Entry) o;
                Object key = serializationService.toObject(entry.getKey());
                Object value = serializationService.toObject(entry.getValue());
                AbstractMap.SimpleImmutableEntry item = new AbstractMap.SimpleImmutableEntry(key, value);
                try {
                    listIterator.set(item);
                } catch (Exception e) {
                    ignore(e);
                }

                return item;
            }
            return o;
        }

    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.UNMODIFIABLE_LAZY_LIST;
    }
}
