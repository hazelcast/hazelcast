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

package com.hazelcast.client;

import com.hazelcast.core.IList;

import java.util.*;

import static com.hazelcast.client.ProxyHelper.check;

public class ListClientProxy<E> extends CollectionClientProxy<E> implements IList<E> {
    public ListClientProxy(HazelcastClient hazelcastClient, String name) {
        super(hazelcastClient, name);
    }

    @Override
    public boolean add(E o) {
        check(o);
//        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_ADD_TO_LIST, o, null);
    return false;
    }

    @Override
    public boolean remove(Object o) {
        check(o);
        return false;
//        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_REMOVE_ITEM, o, null);
    }

    @Override
    public boolean contains(Object o) {
        check(o);
        return false;
//        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS_KEY, o, null);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof IList && o != null) {
            return getName().equals(((IList) o).getName());
        } else {
            return false;
        }
    }

    @Override
    protected Collection<E> getTheCollection() {
        final Collection<Map.Entry<?, E>> entries = proxyHelper.entries(null);
        final Iterator<Map.Entry<?, E>> it = entries.iterator();
        final ListClientProxy thisListProxy = this;
        return new AbstractCollection<E>() {
            @Override
            public Iterator<E> iterator() {
                return new Iterator<E>() {
                    volatile E lastRecord;

                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    public E next() {
                        lastRecord = it.next().getValue();
                        return lastRecord;
                    }

                    public void remove() {
                        if (lastRecord == null) {
                            throw new IllegalStateException();
                        }
                        it.remove();
                        thisListProxy.remove(lastRecord);
                    }
                };
            }

            @Override
            public int size() {
                return entries.size();
            }
        };
    }

    public String getName() {
        return name;
    }

    public void add(int index, E element) {
        throw new UnsupportedOperationException();
    }

    public boolean addAll(int index, Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    public E get(int index) {
        throw new UnsupportedOperationException();
    }

    public int indexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    public ListIterator<E> listIterator() {
        throw new UnsupportedOperationException();
    }

    public ListIterator<E> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    public E remove(int index) {
        throw new UnsupportedOperationException();
    }

    public E set(int index, E element) {
        throw new UnsupportedOperationException();
    }

    public List<E> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }
}
