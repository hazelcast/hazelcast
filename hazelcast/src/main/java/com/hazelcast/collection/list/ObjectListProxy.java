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

package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionProxy;
import com.hazelcast.collection.CollectionProxyType;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.multimap.MultiMapProxySupport;
import com.hazelcast.collection.operations.CollectionResponse;
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

    public ObjectListProxy(String name, CollectionService service, NodeEngine nodeEngine, CollectionProxyType proxyType) {
        super(COLLECTION_LIST_NAME, service, nodeEngine, proxyType,
                nodeEngine.getConfig().getMultiMapConfig("list:" + name).setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST));
        listName = name;
        key = nodeEngine.toData(name);
    }

    public String getName() {
        return listName;
    }

    public void addItemListener(ItemListener<E> listener, boolean includeValue) {
        service.addListener(name, listener, key, includeValue, false);
    }

    public void removeItemListener(ItemListener<E> listener) {
        service.removeListener(name, listener, key);
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
        CollectionResponse result = getAllInternal(key);
        return result.getObjectCollection(nodeEngine).iterator();
    }

    public Object[] toArray() {
        CollectionResponse result = getAllInternal(key);
        return result.getObjectCollection(nodeEngine).toArray();
    }

    public <T> T[] toArray(T[] a) {
        CollectionResponse result = getAllInternal(key);
        Collection<T> col = result.getObjectCollection(nodeEngine);
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
        for (Object o : c) {
            dataSet.add(nodeEngine.toData(o));
        }
        return containsAllInternal(key, dataSet);
    }

    public boolean addAll(Collection<? extends E> c) {
        return addAll(-1, c);
    }

    public boolean addAll(int index, Collection<? extends E> c) {
        return addAllInternal(key, toDataList(c), index);
    }

    public boolean removeAll(Collection<?> c) {
        return false;
    }

    public boolean retainAll(Collection<?> c) {
        return false;
    }

    public void clear() {
        removeInternal(key);
    }

    public E get(int index) {
        Object data = getInternal(key, index);
        return nodeEngine.toObject(data);
    }

    public E set(int index, E element) {
        Data value = nodeEngine.toData(element);
        Object data = setInternal(key, index, value);
        return nodeEngine.toObject(data);
    }

    public void add(int index, E element) {
        Data data = nodeEngine.toData(element);
        putInternal(key, data, index);
    }

    public E remove(int index) {
        Object data = removeInternal(key, index);
        return nodeEngine.toObject(data);
    }

    public int indexOf(Object o) {
        Data dataValue = nodeEngine.toData(o);
        return indexOfInternal(key, dataValue, false);
    }

    public int lastIndexOf(Object o) {
        Data dataValue = nodeEngine.toData(o);
        return indexOfInternal(key, dataValue, true);
    }

    public ListIterator<E> listIterator() {
        CollectionResponse result = getAllInternal(key);
        List list = (List) result.getObjectCollection(nodeEngine);
        return list.listIterator();
    }

    public ListIterator<E> listIterator(int index) {
        CollectionResponse result = getAllInternal(key);
        List list = (List) result.getObjectCollection(nodeEngine);
        return list.listIterator(index);
    }

    public List<E> subList(int fromIndex, int toIndex) {
        CollectionResponse result = getAllInternal(key);
        List list = (List) result.getObjectCollection(nodeEngine);
        return list.subList(fromIndex, toIndex);
    }

    public Object getId() {
        return name;
    }

    private List<Data> toDataList(Collection coll) {
        List<Data> dataList = new ArrayList<Data>(coll.size());
        for (Object obj : coll) {
            dataList.add(nodeEngine.toData(obj));
        }
        return dataList;
    }
}
