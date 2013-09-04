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

package com.hazelcast.collection;

import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.ExceptionUtil;

import java.util.*;
import java.util.concurrent.Future;

/**
 * @ali 9/4/13
 */
public abstract class AbstractCollectionProxyImpl<S extends RemoteService, E> extends AbstractDistributedObject<S> implements InitializingObject {

    protected final String name;
    protected final int partitionId;

    protected AbstractCollectionProxyImpl(String name, NodeEngine nodeEngine, S service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }

    public void initialize() {

    }

    public boolean add(E e) {
        throwExceptionIfNull(e);
        final Data value = getNodeEngine().toData(e);
        final CollectionAddOperation operation = new CollectionAddOperation(name, value);
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
        throwExceptionIfNull(c);
        List<Data> valueList = new ArrayList<Data>(c.size());
        final NodeEngine nodeEngine = getNodeEngine();
        for (E e : c) {
            throwExceptionIfNull(e);
            valueList.add(nodeEngine.toData(e));
        }
        final CollectionAddAllOperation operation = new CollectionAddAllOperation(name, valueList);
        final Boolean result = invoke(operation);
        return result;
    }

    public boolean retainAll(Collection<?> c) {
        return compareAndRemove(true, c);
    }

    public boolean removeAll(Collection<?> c) {
        return compareAndRemove(false, c);
    }

    private boolean compareAndRemove(boolean retain, Collection<?> c){
        throwExceptionIfNull(c);
        Set<Data> valueSet = new HashSet<Data>(c.size());
        final NodeEngine nodeEngine = getNodeEngine();
        for (Object o : c) {
            throwExceptionIfNull(o);
            valueSet.add(nodeEngine.toData(o));
        }
        final CollectionCompareAndRemoveOperation operation = new CollectionCompareAndRemoveOperation(name, retain, valueSet);
        final Boolean result = invoke(operation);
        return result;
    }

    public void clear() {
        final CollectionClearOperation operation = new CollectionClearOperation(name);
        invoke(operation);
    }

    public Iterator<E> iterator() {
        return getAll().iterator();
    }

    public Object[] toArray() {
        return getAll().toArray();
    }

    public <T> T[] toArray(T[] a) {
        return getAll().toArray(a);
    }

    private Collection<E> getAll(){
        final CollectionGetAllOperation operation = new CollectionGetAllOperation(name);
        final SerializableCollection result = invoke(operation);
        final Collection<Data> collection = result.getCollection();
        final List<E> list = new ArrayList<E>(collection.size());
        final NodeEngine nodeEngine = getNodeEngine();
        for (Data data : collection) {
            list.add(nodeEngine.<E>toObject(data));
        }
        return list;
    }

    public String addItemListener(ItemListener<E> listener, boolean includeValue) {
        final EventService eventService = getNodeEngine().getEventService();
        final EventRegistration registration = eventService.registerListener(getServiceName(), name, new CollectionEventFilter(includeValue), listener);
        return registration.getId();
    }

    public boolean removeItemListener(String registrationId) {
        EventService eventService = getNodeEngine().getEventService();
        return eventService.deregisterListener(getServiceName(), name, registrationId);
    }

    protected  <T> T invoke(CollectionOperation operation) {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(getServiceName(), operation, partitionId).build();
            Future f = inv.invoke();
            return nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected void throwExceptionIfNull(Object o) {
        if (o == null) {
            throw new NullPointerException("Object is null");
        }
    }
}
