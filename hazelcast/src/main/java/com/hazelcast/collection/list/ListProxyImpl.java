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

import com.hazelcast.collection.AbstractCollectionProxyImpl;
import com.hazelcast.config.CollectionConfig;
import com.hazelcast.core.IList;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.SerializableCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class ListProxyImpl<E> extends AbstractCollectionProxyImpl<ListService, E> implements IList<E> {

    protected ListProxyImpl(String name, NodeEngine nodeEngine, ListService service) {
        super(name, nodeEngine, service);
    }

    @Override
    protected CollectionConfig getConfig(NodeEngine nodeEngine) {
        return nodeEngine.getConfig().findListConfig(name);
    }

    @Override
    public void add(int index, E e) {
        throwExceptionIfNull(e);
        throwExceptionIfNegative(index);
        final Data value = getNodeEngine().toData(e);
        final ListAddOperation operation = new ListAddOperation(name, index, value);
        invoke(operation);
    }

    @Override
    public E get(int index) {
        throwExceptionIfNegative(index);
        final ListGetOperation operation = new ListGetOperation(name, index);
        return invoke(operation);
    }

    @Override
    public E set(int index, E element) {
        throwExceptionIfNull(element);
        throwExceptionIfNegative(index);
        final Data value = getNodeEngine().toData(element);
        final ListSetOperation operation = new ListSetOperation(name, index, value);
        return invoke(operation);
    }

    @Override
    public E remove(int index) {
        throwExceptionIfNegative(index);
        final ListRemoveOperation operation = new ListRemoveOperation(name, index);
        return invoke(operation);
    }

    @Override
    public int indexOf(Object o) {
        return indexOfInternal(false, o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return indexOfInternal(true, o);
    }

    private int indexOfInternal(boolean last, Object o) {
        throwExceptionIfNull(o);
        final Data value = getNodeEngine().toData(o);
        final ListIndexOfOperation operation = new ListIndexOfOperation(name, last, value);
        final Integer result = invoke(operation);
        return result;
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throwExceptionIfNull(c);
        throwExceptionIfNegative(index);
        List<Data> valueList = new ArrayList<Data>(c.size());
        final NodeEngine nodeEngine = getNodeEngine();
        for (E e : c) {
            throwExceptionIfNull(e);
            valueList.add(nodeEngine.toData(e));
        }
        final ListAddAllOperation operation = new ListAddAllOperation(name, index, valueList);
        final Boolean result = invoke(operation);
        return result;
    }

    @Override
    public ListIterator<E> listIterator() {
        return listIterator(0);
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        final List<E> list = subList(-1, -1);
        return list.listIterator(index);
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        final ListSubOperation operation = new ListSubOperation(name, fromIndex, toIndex);
        final SerializableCollection result = invoke(operation);
        final Collection<Data> collection = result.getCollection();
        final List<E> list = new ArrayList<E>(collection.size());
        final NodeEngine nodeEngine = getNodeEngine();
        for (Data data : collection) {
            list.add(nodeEngine.<E>toObject(data));
        }
        return list;
    }

    @Override
    public Iterator<E> iterator() {
        return listIterator(0);
    }

    @Override
    public Object[] toArray() {
        return subList(-1, -1).toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throwExceptionIfNull(a);
        return subList(-1, -1).toArray(a);
    }

    @Override
    public String getServiceName() {
        return ListService.SERVICE_NAME;
    }

}
