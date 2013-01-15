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

package com.hazelcast.util;

import java.util.*;

public class SetFromMap<E> extends AbstractSet<E> implements Set<E> {

    private final Map<E, Boolean> map;

    public SetFromMap(final Map<E, Boolean> map) {
        super();
        this.map = map;
    }

    public void clear() {
        map.clear();
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public boolean contains(Object o) {
        return map.containsKey(o);
    }

    public boolean remove(Object o) {
        return map.remove(o) != null;
    }

    public boolean add(E e) {
        return map.put(e, Boolean.TRUE) == null;
    }

    public Iterator<E> iterator() {
        return map.keySet().iterator();
    }

    public Object[] toArray() {
        return map.keySet().toArray();
    }

    public <T> T[] toArray(T[] a) {
        return map.keySet().toArray(a);
    }

    public String toString() {
        return map.keySet().toString();
    }

    public int hashCode() {
        return map.keySet().hashCode();
    }

    public boolean equals(Object o) {
        return o == this || map.keySet().equals(o);
    }

    public boolean containsAll(Collection<?> c) {
        return map.keySet().containsAll(c);
    }

    public boolean removeAll(Collection<?> c) {
        return map.keySet().removeAll(c);
    }

    public boolean retainAll(Collection<?> c) {
        return map.keySet().retainAll(c);
    }
}
