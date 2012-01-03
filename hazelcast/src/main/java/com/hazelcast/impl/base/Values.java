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

package com.hazelcast.impl.base;

import com.hazelcast.impl.concurrentmap.ValueHolder;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static com.hazelcast.nio.IOUtil.toObject;

public class Values implements Collection, DataSerializable {
    Collection<Data> lsValues = null;

    public Values() {
    }

    public Values(Collection<ValueHolder> values) {
        super();
        if (values != null) {
            this.lsValues = new ArrayList<Data>(values.size());
            for (ValueHolder valueHolder : values) {
                if (valueHolder != null) {
                    lsValues.add(valueHolder.getData());
                }
            }
        }
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
        if (o == null) {
            throw new IllegalArgumentException("Contains cannot have null argument.");
        }
        Iterator it = iterator();
        while (it.hasNext()) {
            Object v = it.next();
            if (o.equals(v)) {
                return true;
            }
        }
        return false;
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
        final Iterator<Data> it;

        public ValueIterator(Iterator<Data> it) {
            super();
            this.it = it;
        }

        public boolean hasNext() {
            return it.hasNext();
        }

        public Object next() {
            Data value = it.next();
            return toObject(value);
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
        Iterator<Data> it = lsValues.iterator();
        int index = 0;
        while (it.hasNext()) {
            a[index++] = toObject(it.next());
        }
        return a;
    }

    public void readData(DataInput in) throws IOException {
        int size = in.readInt();
        lsValues = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            Data data = new Data();
            data.readData(in);
            lsValues.add(data);
        }
    }

    public void writeData(DataOutput out) throws IOException {
        int size = (lsValues == null) ? 0 : lsValues.size();
        out.writeInt(size);
        if (size > 0) {
            for (Data data : lsValues) {
                data.writeData(out);
            }
        }
    }

    public String toString() {
        Iterator i = iterator();
        if (!i.hasNext())
            return "[]";
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (; ; ) {
            Object e = i.next();
            sb.append(e == this ? "(this Collection)" : e);
            if (!i.hasNext())
                return sb.append(']').toString();
            sb.append(", ");
        }
    }
}
