/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.util;

import java.util.*;
import java.util.Map.Entry;

public class ValueCollection<K, V> implements Collection<V> {
    private final EntryHolder<K, V> proxy;
    private final Set<Entry<K, V>> entrySet;

    public ValueCollection(EntryHolder<K, V> proxy, Set<Entry<K, V>> entrySet) {
        this.proxy = proxy;
        this.entrySet = entrySet;
    }

    public boolean add(V arg0) {
        throw new UnsupportedOperationException();
    }

    public boolean addAll(Collection<? extends V> arg0) {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        throw new UnsupportedOperationException();
    }

    public boolean contains(Object arg0) {
        for (Entry<K, V> anEntrySet : entrySet) {
            Object object = anEntrySet.getValue();
            if (object.equals(arg0)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsAll(Collection<?> arg0) {
        for (Object object : arg0) {
            if (!contains(object)) {
                return false;
            }
        }
        return true;
    }

    public boolean isEmpty() {
        return entrySet.size() == 0;
    }

    public Iterator<V> iterator() {
        return new ValueIterator<K, V>(entrySet.iterator());
    }

    public boolean remove(Object arg0) {
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(Collection<?> arg0) {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(Collection<?> arg0) {
        return false;
    }

    public int size() {
        return entrySet.size();
    }

    public Object[] toArray() {
        List<V> list = new ArrayList<V>(entrySet.size());
        for (Entry<K, V> entry : entrySet) {
            list.add(entry.getValue());
        }
        return list.toArray();
    }

    public <T> T[] toArray(final T[] a) {
        if (a == null) {
            throw new NullPointerException();
        }
        final int size = size();
        final T[] result;
        if (a.length < size) {
            result = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
        } else {
            result = a;
        }
        int i = 0;
        for (Entry<K, V> entry : entrySet) {
            result[i++] = (T) entry.getValue();
        }
        for (int j = i; j < a.length; j++) {
            result[j] = null;
        }
        return result;
    }
}
