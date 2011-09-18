/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client.impl;

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.client.IOUtil.toObject;

public class Values<V> implements Collection, DataSerializable {
    List<V> lsValues = null;

    public Values() {
    }

    public boolean add(Object o) {
        throw new UnsupportedOperationException();
    }

    public boolean addAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        throw new UnsupportedOperationException();
    }

    public boolean contains(Object o) {
        return lsValues.contains(o);
    }

    public boolean containsAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty() {
        return (size() == 0);
    }

    public Iterator iterator() {
        return new ValueIterator(lsValues.iterator());
    }

    class ValueIterator implements Iterator {
        final Iterator<V> it;

        public ValueIterator(Iterator<V> it) {
            super();
            this.it = it;
        }

        public boolean hasNext() {
            return it.hasNext();
        }

        public Object next() {
            V value = it.next();
            return value;
        }

        public void remove() {
            it.remove();
        }
    }

    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public int size() {
        return (lsValues == null) ? 0 : lsValues.size();
    }

    public Object[] toArray() {
        if (size() == 0) {
            return null;
        }
        return toArray(new Object[size()]);
    }

    public Object[] toArray(Object[] a) {
        int size = size();
        if (size == 0) {
            return null;
        }
        if (a == null || a.length < size) {
            a = new Object[size];
        }
        for (int i = 0; i < size; i++) {
            a[i] = lsValues.get(i);
        }
        return a;
    }

    public void readData(DataInput in) throws IOException {
        int size = in.readInt();
        lsValues = new ArrayList<V>(size);
        List<byte[]> lsValuesInByte = new ArrayList<byte[]>(size);
        for (int i = 0; i < size; i++) {
            int length = in.readInt();
            byte[] data = new byte[length];
            in.readFully(data);  // buffer of Data
            in.readInt(); // partitionHash of Data
            lsValuesInByte.add(data);
        }
        for (int i = 0; i < size; i++) {
            lsValues.add((V) toObject(lsValuesInByte.get(i)));
        }
    }

    public void writeData(DataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }
}
