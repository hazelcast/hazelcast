/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.collection;

import com.hazelcast.collection.impl.collection.operations.CollectionAddAllOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionAddOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionClearOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionCompareAndRemoveOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionContainsOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionGetAllOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionIsEmptyOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionRemoveOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionSizeOperation;
import com.hazelcast.config.CollectionConfig;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.SerializableList;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.util.Preconditions.checkNotNull;

public abstract class AbstractCollectionProxyImpl<S extends RemoteService, E> extends AbstractDistributedObject<S>
        implements InitializingObject {

    protected final String name;
    protected final int partitionId;

    protected AbstractCollectionProxyImpl(String name, NodeEngine nodeEngine, S service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    @Override
    public void initialize() {
        final NodeEngine nodeEngine = getNodeEngine();
        CollectionConfig config = getConfig(nodeEngine);
        final List<ItemListenerConfig> itemListenerConfigs = config.getItemListenerConfigs();
        for (ItemListenerConfig itemListenerConfig : itemListenerConfigs) {
            ItemListener listener = itemListenerConfig.getImplementation();
            if (listener == null && itemListenerConfig.getClassName() != null) {
                try {
                    listener = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(), itemListenerConfig.getClassName());
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
            if (listener != null) {
                if (listener instanceof HazelcastInstanceAware) {
                    ((HazelcastInstanceAware) listener).setHazelcastInstance(nodeEngine.getHazelcastInstance());
                }
                addItemListener(listener, itemListenerConfig.isIncludeValue());
            }
        }
    }

    protected abstract CollectionConfig getConfig(NodeEngine nodeEngine);

    @Override
    public String getName() {
        return name;
    }

    public boolean add(E e) {
        checkObjectNotNull(e);
        final Data value = getNodeEngine().toData(e);
        final CollectionAddOperation operation = new CollectionAddOperation(name, value);
        final Boolean result = invoke(operation);
        return result;
    }

    public boolean remove(Object o) {
        checkObjectNotNull(o);

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
        final CollectionIsEmptyOperation operation = new CollectionIsEmptyOperation(name);
        final Boolean result = invoke(operation);
        return result;
    }

    public boolean contains(Object o) {
        checkObjectNotNull(o);

        Set<Data> valueSet = new HashSet<Data>(1);
        valueSet.add(getNodeEngine().toData(o));
        final CollectionContainsOperation operation = new CollectionContainsOperation(name, valueSet);
        final Boolean result = invoke(operation);
        return result;
    }

    public boolean containsAll(Collection<?> c) {
        checkObjectNotNull(c);

        Set<Data> valueSet = new HashSet<Data>(c.size());
        final NodeEngine nodeEngine = getNodeEngine();
        for (Object o : c) {
            checkObjectNotNull(o);
            valueSet.add(nodeEngine.toData(o));
        }
        final CollectionContainsOperation operation = new CollectionContainsOperation(name, valueSet);
        final Boolean result = invoke(operation);
        return result;
    }

    public boolean addAll(Collection<? extends E> c) {
        checkObjectNotNull(c);

        List<Data> valueList = new ArrayList<Data>(c.size());
        final NodeEngine nodeEngine = getNodeEngine();
        for (E e : c) {
            checkObjectNotNull(e);
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

    private boolean compareAndRemove(boolean retain, Collection<?> c) {
        checkObjectNotNull(c);

        Set<Data> valueSet = new HashSet<Data>(c.size());
        final NodeEngine nodeEngine = getNodeEngine();
        for (Object o : c) {
            checkObjectNotNull(o);
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
        return Collections.unmodifiableCollection(getAll()).iterator();
    }

    public Object[] toArray() {
        return getAll().toArray();
    }

    public <T> T[] toArray(T[] a) {
        return getAll().toArray(a);
    }

    private Collection<E> getAll() {
        CollectionGetAllOperation operation = new CollectionGetAllOperation(name);
        SerializableList result = invoke(operation);
        List<Data> collection = result.getCollection();
        SerializationService serializationService = getNodeEngine().getSerializationService();
        return new UnmodifiableLazyList<E>(collection, serializationService);
    }

    public String addItemListener(ItemListener<E> listener, boolean includeValue) {
        final EventService eventService = getNodeEngine().getEventService();
        final CollectionEventFilter filter = new CollectionEventFilter(includeValue);
        final EventRegistration registration = eventService.registerListener(getServiceName(), name, filter, listener);
        return registration.getId();
    }

    public boolean removeItemListener(String registrationId) {
        EventService eventService = getNodeEngine().getEventService();
        return eventService.deregisterListener(getServiceName(), name, registrationId);
    }

    protected <T> T invoke(CollectionOperation operation) {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Future f = nodeEngine.getOperationService().invokeOnPartition(getServiceName(), operation, partitionId);
            return nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected void checkObjectNotNull(Object o) {
        checkNotNull(o, "Object is null");
    }

    protected void checkIndexNotNegative(int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException("Index is negative");
        }
    }
}
