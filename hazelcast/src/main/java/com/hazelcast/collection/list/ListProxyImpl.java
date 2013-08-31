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

import com.hazelcast.collection.operation.CollectionOperation;
import com.hazelcast.core.IList;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
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

    public boolean remove(Object o) {
        return false;
    }

    public boolean containsAll(Collection<?> c) {
        return false;
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
        final GetOperation operation = new GetOperation(name, index);
        return invoke(operation);
    }

    public E set(int index, E element) {
        return null;
    }

    public void add(int index, E element) {
        addInternal(index, element);
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

    public int size() {
        return 0;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean contains(Object o) {
        return false;
    }

    public Iterator<E> iterator() {
        return null;
    }

    public Object[] toArray() {
        return new Object[0];
    }

    public <T> T[] toArray(T[] a) {
        return null;
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

    private boolean addInternal(int index, E e){
        throwExceptionIfNull(e);
        final Data value = getNodeEngine().toData(e);
        final AddOperation operation = new AddOperation(name, index, value);
        Boolean result = invoke(operation);
        return result;
    }
}
