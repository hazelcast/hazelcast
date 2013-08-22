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
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.multimap.MultiMapProxySupport;
import com.hazelcast.collection.operations.CollectionResponse;
import com.hazelcast.core.IList;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.*;

/**
 * @author ali 1/14/13
 */
public class ObjectListProxy<E> extends MultiMapProxySupport implements CollectionProxy, IList<E> {


    final Data key;

    public ObjectListProxy(CollectionService service, NodeEngine nodeEngine, CollectionProxyId proxyId) {
        super(service, nodeEngine, createConfig(proxyId), proxyId);
        key = nodeEngine.toData(createCollectionKey(proxyId));
    }

    public String getName() {
        return proxyId.getKeyName();
    }

    public String addItemListener(ItemListener<E> listener, boolean includeValue) {
        return getService().addListener(proxyId.getName(), listener, key, includeValue, false);
    }

    public boolean removeItemListener(String registrationId) {
        return getService().removeListener(proxyId.getName(), registrationId);
    }

    public int size() {
        return countInternal(key);
    }

    public boolean isEmpty() {
        return countInternal(key) == 0;
    }

    public boolean contains(Object o) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data data = nodeEngine.toData(o);
        return containsInternalList(key, data);
    }

    public Iterator<E> iterator() {
        final NodeEngine nodeEngine = getNodeEngine();
        CollectionResponse result = getAllInternal(key);
        return result.getObjectCollection(nodeEngine).iterator();
    }

    public Object[] toArray() {
        final NodeEngine nodeEngine = getNodeEngine();
        CollectionResponse result = getAllInternal(key);
        return result.getObjectCollection(nodeEngine).toArray();
    }

    public <T> T[] toArray(T[] a) {
        final NodeEngine nodeEngine = getNodeEngine();
        CollectionResponse result = getAllInternal(key);
        Collection<T> col = result.getObjectCollection(nodeEngine);
        return col.toArray(a);
    }

    public boolean add(E e) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data data = nodeEngine.toData(e);
        return putInternal(key, data, -1);
    }

    public boolean remove(Object o) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data data = nodeEngine.toData(o);
        return removeInternal(key, data);
    }

    public boolean containsAll(Collection<?> c) {
        final NodeEngine nodeEngine = getNodeEngine();
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
        return compareAndRemoveInternal(key, toDataList(c), false);
    }

    public boolean retainAll(Collection<?> c) {
        return compareAndRemoveInternal(key, toDataList(c), true);
    }

    public void clear() {
        removeInternal(key);
    }

    public E get(int index) {
        final NodeEngine nodeEngine = getNodeEngine();
        Object data = getInternal(key, index);
        return nodeEngine.toObject(data);
    }

    public E set(int index, E element) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data value = nodeEngine.toData(element);
        Object data = setInternal(key, index, value);
        return nodeEngine.toObject(data);
    }

    public void add(int index, E element) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data data = nodeEngine.toData(element);
        putInternal(key, data, index);
    }

    public E remove(int index) {
        final NodeEngine nodeEngine = getNodeEngine();
        Object data = removeInternal(key, index);
        return nodeEngine.toObject(data);
    }

    public int indexOf(Object o) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data dataValue = nodeEngine.toData(o);
        return indexOfInternal(key, dataValue, false);
    }

    public int lastIndexOf(Object o) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data dataValue = nodeEngine.toData(o);
        return indexOfInternal(key, dataValue, true);
    }

    public ListIterator<E> listIterator() {
        final NodeEngine nodeEngine = getNodeEngine();
        CollectionResponse result = getAllInternal(key);
        List list = (List) result.getObjectCollection(nodeEngine);
        return list.listIterator();
    }

    public ListIterator<E> listIterator(int index) {
        final NodeEngine nodeEngine = getNodeEngine();
        CollectionResponse result = getAllInternal(key);
        List list = (List) result.getObjectCollection(nodeEngine);
        return list.listIterator(index);
    }

    public List<E> subList(int fromIndex, int toIndex) {
        final NodeEngine nodeEngine = getNodeEngine();
        CollectionResponse result = getAllInternal(key);
        List list = (List) result.getObjectCollection(nodeEngine);
        return list.subList(fromIndex, toIndex);
    }

    public Object getId() {
        return proxyId;
    }

    private List<Data> toDataList(Collection coll) {
        final NodeEngine nodeEngine = getNodeEngine();
        List<Data> dataList = new ArrayList<Data>(coll.size());
        for (Object obj : coll) {
            dataList.add(nodeEngine.toData(obj));
        }
        return dataList;
    }
}
