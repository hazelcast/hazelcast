/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionProxy;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.multimap.MultiMapCollectionResponse;
import com.hazelcast.collection.multimap.MultiMapProxySupport;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.IList;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.*;

/**
 * @ali 1/14/13
 */
public class ObjectListProxy<E> extends MultiMapProxySupport implements CollectionProxy, IList<E> {

    public static final String COLLECTION_LIST_NAME = "hz:collection:name:list";

    final String listName;

    final Data key;

    public ObjectListProxy(String name, CollectionService service, NodeEngine nodeEngine, CollectionProxyId proxyId) {
        super(COLLECTION_LIST_NAME, service, nodeEngine, proxyId,
                nodeEngine.getConfig().getMultiMapConfig("list:" + name).setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST));
        listName = name;
        key = nodeEngine.toData(name);
    }

    public String getName() {
        return listName;
    }

    public void addItemListener(ItemListener<E> listener, boolean includeValue) {

    }

    public void removeItemListener(ItemListener<E> listener) {

    }

    public int size() {
        return countInternal(key);
    }

    public boolean isEmpty() {
        return countInternal(key) == 0;
    }

    public boolean contains(Object o) {
        Data data = nodeEngine.toData(o);
        return containsInternalList(key, data);
    }

    public Iterator<E> iterator() {
        MultiMapCollectionResponse result = getAllInternal(key);
        return result.getObjectCollection(nodeEngine.getSerializationService()).iterator();
    }

    public Object[] toArray() {
        MultiMapCollectionResponse result = getAllInternal(key);
        return result.getObjectCollection(nodeEngine.getSerializationService()).toArray();
    }

    public <T> T[] toArray(T[] a) {
        MultiMapCollectionResponse result = getAllInternal(key);
        Collection<T> col = result.getObjectCollection(nodeEngine.getSerializationService());
        return col.toArray(a);
    }

    public boolean add(E e) {
        Data data = nodeEngine.toData(e);
        return putInternal(key, data, -1);
    }

    public boolean remove(Object o) {
        Data data = nodeEngine.toData(o);
        return removeInternal(key, data);
    }

    public boolean containsAll(Collection<?> c) {
        Set<Data> dataSet = new HashSet<Data>(c.size());
        for (Object o: c){
            dataSet.add(nodeEngine.toData(o));
        }
        return containsAllInternal(key, dataSet);
    }

    public boolean addAll(Collection<? extends E> c) {
        return false;
    }

    public boolean addAll(int index, Collection<? extends E> c) {
        return false;
    }

    public boolean removeAll(Collection<?> c) {
        return false;
    }

    public boolean retainAll(Collection<?> c) {
        return false;
    }

    public void clear() {

    }

    public E get(int index) {
        Data data = getInternal(key, index);
        if (data == null) {
            throw new IndexOutOfBoundsException();
        }
        return nodeEngine.toObject(data);
    }

    public E set(int index, E element) {
        return null;
    }

    public void add(int index, E element) {
        Data data = nodeEngine.toData(element);
        putInternal(key, data, index);
    }

    public E remove(int index) {
        return null;
    }

    public int indexOf(Object o) {
        return 0;
    }

    public int lastIndexOf(Object o) {
        return 0;
    }

    public ListIterator<E> listIterator() {
        return null;
    }

    public ListIterator<E> listIterator(int index) {
        return null;
    }

    public List<E> subList(int fromIndex, int toIndex) {
        return null;
    }

    public Object getId() {
        return null;
    }
}
