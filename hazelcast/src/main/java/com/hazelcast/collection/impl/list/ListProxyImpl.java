/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import javax.annotation.Nonnull;

import com.hazelcast.collection.IList;
import com.hazelcast.collection.LocalListStats;
import com.hazelcast.collection.impl.collection.AbstractCollectionProxyImpl;
import com.hazelcast.collection.impl.list.operations.ListAddAllOperation;
import com.hazelcast.collection.impl.list.operations.ListAddOperation;
import com.hazelcast.collection.impl.list.operations.ListGetOperation;
import com.hazelcast.collection.impl.list.operations.ListIndexOfOperation;
import com.hazelcast.collection.impl.list.operations.ListRemoveOperation;
import com.hazelcast.collection.impl.list.operations.ListSetOperation;
import com.hazelcast.collection.impl.list.operations.ListSubOperation;
import com.hazelcast.config.CollectionConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.SerializableList;
import com.hazelcast.spi.impl.UnmodifiableLazyList;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class ListProxyImpl<E> extends AbstractCollectionProxyImpl<ListService, E> implements IList<E> {

    protected ListProxyImpl(String name, NodeEngine nodeEngine, ListService service) {
        super(name, nodeEngine, service);
    }

    @Override
    protected CollectionConfig getConfig(NodeEngine nodeEngine) {
        return nodeEngine.getConfig().findListConfig(name);
    }

    @Override
    public void add(int index, @Nonnull E e) {
        checkNotNull(e, "Null item is not allowed");
        checkIndexNotNegative(index);

        final Data value = getNodeEngine().toData(e);
        final ListAddOperation operation = new ListAddOperation(name, index, value);
        invoke(operation);
    }

    @Override
    public E get(int index) {
        checkIndexNotNegative(index);

        final ListGetOperation operation = new ListGetOperation(name, index);
        return invoke(operation);
    }

    @Override
    public E set(int index, @Nonnull E element) {
        checkNotNull(element, "Null item is not allowed");
        checkIndexNotNegative(index);

        final Data value = getNodeEngine().toData(element);
        final ListSetOperation operation = new ListSetOperation(name, index, value);
        return invoke(operation);
    }

    @Override
    public E remove(int index) {
        checkIndexNotNegative(index);

        final ListRemoveOperation operation = new ListRemoveOperation(name, index);
        return invoke(operation);
    }

    @Override
    public int indexOf(@Nonnull Object o) {
        return indexOfInternal(false, o);
    }

    @Override
    public int lastIndexOf(@Nonnull Object o) {
        return indexOfInternal(true, o);
    }

    private int indexOfInternal(boolean last, @Nonnull Object o) {
        checkNotNull(o, "Null item is not allowed");

        final Data value = getNodeEngine().toData(o);
        final ListIndexOfOperation operation = new ListIndexOfOperation(name, last, value);
        final Integer result = invoke(operation);
        return result;
    }

    @Override
    public boolean addAll(int index, @Nonnull Collection<? extends E> c) {
        checkNotNull(c, "Null collection is not allowed");
        checkIndexNotNegative(index);

        List<Data> valueList = new ArrayList<Data>(c.size());
        final NodeEngine nodeEngine = getNodeEngine();
        for (E e : c) {
            checkNotNull(e, "Null collection element is not allowed");
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
        ListSubOperation operation = new ListSubOperation(name, fromIndex, toIndex);
        SerializableList result = invoke(operation);
        List<Data> collection = result.getCollection();
        SerializationService serializationService = getNodeEngine().getSerializationService();
        return new UnmodifiableLazyList(collection, serializationService);
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
    public <T> T[] toArray(@Nonnull T[] a) {
        checkNotNull(a, "Null array parameter is not allowed!");
        return subList(-1, -1).toArray(a);
    }

    @Override
    public String getServiceName() {
        return ListService.SERVICE_NAME;
    }

    @Override
    public LocalListStats getLocalListStats() {
        return getService().getLocalCollectionStats(name);
    }

    // used by jet
    public Iterator<Data> dataIterator() {
        return dataSubList(-1, -1).listIterator();
    }

    // used by jet
    public List<Data> dataSubList(int fromIndex, int toIndex) {
        ListSubOperation operation = new ListSubOperation(name, fromIndex, toIndex);
        SerializableList result = invoke(operation);
        return Collections.unmodifiableList(result.getCollection());
    }
}
