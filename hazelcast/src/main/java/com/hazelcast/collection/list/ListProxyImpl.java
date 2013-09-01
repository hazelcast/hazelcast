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

import com.hazelcast.collection.CollectionClearOperation;
import com.hazelcast.collection.CollectionContainsOperation;
import com.hazelcast.collection.CollectionRemoveOperation;
import com.hazelcast.collection.CollectionSizeOperation;
import com.hazelcast.collection.operation.CollectionOperation;
import com.hazelcast.core.IList;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.ExceptionUtil;

import java.util.*;
import java.util.concurrent.Future;

/**
 * @ali 8/30/13
 */
public class ListProxyImpl<E> extends AbstractDistributedObject<ListService> implements IList<E>, InitializingObject {

    final String name;
    final int partitionId;

    protected ListProxyImpl(String name, NodeEngine nodeEngine, ListService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    public boolean add(E e) {
        return addInternal(-1, e);
    }

    public void add(int index, E element) {
        addInternal(index, element);
    }

    private boolean addInternal(int index, E e){
        throwExceptionIfNull(e);
        final Data value = getNodeEngine().toData(e);
        final ListAddOperation operation = new ListAddOperation(name, index, value);
        final Boolean result = invoke(operation);
        return result;
    }

    public boolean remove(Object o) {
        throwExceptionIfNull(o);
        final Data value = getNodeEngine().toData(o);
        final CollectionRemoveOperation operation = new CollectionRemoveOperation(name, value);
        final Boolean result = invoke(operation);
        return result;
    }

    public void clear() {
        final CollectionClearOperation operation = new CollectionClearOperation(name);
        invoke(operation);
    }

    public E get(int index) {
        final ListGetOperation operation = new ListGetOperation(name, index);
        return invoke(operation);
    }

    public E set(int index, E element) {
        throwExceptionIfNull(element);
        final Data value = getNodeEngine().toData(element);
        final ListSetOperation operation = new ListSetOperation(name, index, value);
        return invoke(operation);
    }

    public E remove(int index) {
        final ListRemoveOperation operation = new ListRemoveOperation(name, index);
        return invoke(operation);
    }

    public int indexOf(Object o) {
        return indexOfInternal(false, o);
    }

    public int lastIndexOf(Object o) {
        return indexOfInternal(true, o);
    }

    private int indexOfInternal(boolean last, Object o){
        throwExceptionIfNull(o);
        final Data value = getNodeEngine().toData(o);
        final ListIndexOfOperation operation = new ListIndexOfOperation(name, last, value);
        final Integer result = invoke(operation);
        return result;
    }

    public boolean containsAll(Collection<?> c) {
        throwExceptionIfNull(c);
        Set<Data> valueSet = new HashSet<Data>(c.size());
        final NodeEngine nodeEngine = getNodeEngine();
        for (Object o : c) {
            throwExceptionIfNull(o);
            valueSet.add(nodeEngine.toData(o));
        }
        final CollectionContainsOperation operation = new CollectionContainsOperation(name, valueSet);
        final Boolean result = invoke(operation);
        return result;
    }

    public boolean addAll(Collection<? extends E> c) {
        return addAll(0, c);
    }

    public boolean addAll(int index, Collection<? extends E> c) {
        throwExceptionIfNull(c);
        Set<Data> valueSet = new HashSet<Data>(c.size());
        final NodeEngine nodeEngine = getNodeEngine();
        for (E e : c) {
            throwExceptionIfNull(e);
            valueSet.add(nodeEngine.toData(e));
        }
        final ListAddAllOperation operation = new ListAddAllOperation(name, index, valueSet);
        final Boolean result = invoke(operation);
        return result;
    }

    public boolean removeAll(Collection<?> c) {
        return false;
    }

    public boolean retainAll(Collection<?> c) {
        return false;
    }

    public ListIterator<E> listIterator() {
        return listIterator(0);
    }

    public ListIterator<E> listIterator(int index) {
        final List<E> list = subList(-1, -1);
        return list.listIterator(index);
    }

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

    public int size() {
        final CollectionSizeOperation operation = new CollectionSizeOperation(name);
        final Integer result = invoke(operation);
        return result;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean contains(Object o) {
        throwExceptionIfNull(o);
        Set<Data> valueSet = new HashSet<Data>(1);
        valueSet.add(getNodeEngine().toData(o));
        final CollectionContainsOperation operation = new CollectionContainsOperation(name, valueSet);
        final Boolean result = invoke(operation);
        return result;
    }

    public Iterator<E> iterator() {
        return listIterator(0);
    }

    public Object[] toArray() {
        return subList(-1, -1).toArray();
    }

    public <T> T[] toArray(T[] a) {
        throwExceptionIfNull(a);
        return subList(-1,-1).toArray(a);
    }

    public String getName() {
        return name;
    }

    public String addItemListener(ItemListener<E> listener, boolean includeValue) {
        return null;
    }

    public boolean removeItemListener(String registrationId) {
        return false;
    }


    public Object getId() {
        return name;
    }

    public String getServiceName() {
        return ListService.SERVICE_NAME;
    }

    public void initialize() {
        //TODO
    }

    private <T> T invoke(CollectionOperation operation) {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(ListService.SERVICE_NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    private void throwExceptionIfNull(Object o) {
        if (o == null) {
            throw new NullPointerException("Object is null");
        }
    }

}
