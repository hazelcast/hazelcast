/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.list;

import com.hazelcast.collection.impl.collection.AbstractCollectionProxyImpl;
import com.hazelcast.collection.impl.list.operations.ListAddAllOperation;
import com.hazelcast.collection.impl.list.operations.ListAddOperation;
import com.hazelcast.collection.impl.list.operations.ListGetOperation;
import com.hazelcast.collection.impl.list.operations.ListIndexOfOperation;
import com.hazelcast.collection.impl.list.operations.ListRemoveOperation;
import com.hazelcast.collection.impl.list.operations.ListSetOperation;
import com.hazelcast.collection.impl.list.operations.ListSubOperation;
import com.hazelcast.config.CollectionConfig;
import com.hazelcast.core.IList;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.SerializableList;
import com.hazelcast.spi.impl.UnmodifiableLazyList;

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
        checkObjectNotNull(e);
        checkIndexNotNegative(index);

        Operation operation = new ListAddOperation(name, index, toData(e))
                .setPartitionId(partitionId);
        invokeOnPartition(operation).join();
    }

    @Override
    public E get(int index) {
        checkIndexNotNegative(index);

        Operation operation = new ListGetOperation(name, index)
                .setPartitionId(partitionId);
        return (E) invokeOnPartition(operation).join();
    }

    @Override
    public E set(int index, E element) {
        checkObjectNotNull(element);
        checkIndexNotNegative(index);

        Operation operation = new ListSetOperation(name, index, toData(element))
                .setPartitionId(partitionId);
        return (E) invokeOnPartition(operation).join();
    }

    @Override
    public E remove(int index) {
        checkIndexNotNegative(index);

        Operation operation = new ListRemoveOperation(name, index)
                .setPartitionId(partitionId);
        return (E) invokeOnPartition(operation).join();
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
        checkObjectNotNull(o);

        Operation operation = new ListIndexOfOperation(name, last, toData(o))
                .setPartitionId(partitionId);
        return (Integer) invokeOnPartition(operation).join();
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        checkObjectNotNull(c);
        checkIndexNotNegative(index);

        List<Data> valueList = new ArrayList<Data>(c.size());
        for (E e : c) {
            checkObjectNotNull(e);
            valueList.add(toData(e));
        }
        Operation operation = new ListAddAllOperation(name, index, valueList)
                .setPartitionId(partitionId);
        return (Boolean) invokeOnPartition(operation).join();
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
        Operation operation = new ListSubOperation(name, fromIndex, toIndex)
                .setPartitionId(partitionId);
        SerializableList result = (SerializableList) invokeOnPartition(operation).join();
        List<Data> collection = result.getCollection();
        SerializationService serializationService = getNodeEngine().getSerializationService();
        return new UnmodifiableLazyList<E>(collection, serializationService);
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
        checkObjectNotNull(a);
        return subList(-1, -1).toArray(a);
    }

    @Override
    public String getServiceName() {
        return ListService.SERVICE_NAME;
    }

}
