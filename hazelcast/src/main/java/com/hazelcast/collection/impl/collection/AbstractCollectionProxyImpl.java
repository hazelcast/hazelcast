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

package com.hazelcast.collection.impl.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import com.hazelcast.collection.ItemListener;
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
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.InitializingObject;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.SerializableList;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;

import static com.hazelcast.internal.config.ConfigValidator.checkCollectionConfig;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static java.util.Collections.singleton;

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
        checkCollectionConfig(config, nodeEngine.getSplitBrainMergePolicyProvider());

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

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public boolean add(@Nonnull E e) {
        checkNotNull(e, "Null item is not allowed");
        final Data value = getNodeEngine().toData(e);
        final CollectionAddOperation operation = new CollectionAddOperation(name, value);
        final Boolean result = invoke(operation);
        return result;
    }

    public boolean remove(@Nonnull Object o) {
        checkNotNull(o, "Null item is not allowed");

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

    public boolean contains(@Nonnull Object o) {
        checkNotNull(o, "Null item is not allowed");

        final CollectionContainsOperation operation = new CollectionContainsOperation(name,
                singleton(getNodeEngine().toData(o)));
        final Boolean result = invoke(operation);
        return result;
    }

    public boolean containsAll(@Nonnull Collection<?> c) {
        checkNotNull(c, "Null collection is not allowed");

        Set<Data> valueSet = createHashSet(c.size());
        final NodeEngine nodeEngine = getNodeEngine();
        for (Object o : c) {
            checkNotNull(o, "Null collection element is not allowed");
            valueSet.add(nodeEngine.toData(o));
        }
        final CollectionContainsOperation operation = new CollectionContainsOperation(name, valueSet);
        final Boolean result = invoke(operation);
        return result;
    }

    public boolean addAll(@Nonnull Collection<? extends E> c) {
        checkNotNull(c, "Null collection is not allowed");

        List<Data> valueList = new ArrayList<Data>(c.size());
        final NodeEngine nodeEngine = getNodeEngine();
        for (E e : c) {
            checkNotNull(e, "Null collection element is not allowed");
            valueList.add(nodeEngine.toData(e));
        }
        final CollectionAddAllOperation operation = new CollectionAddAllOperation(name, valueList);
        final Boolean result = invoke(operation);
        return result;
    }

    public boolean retainAll(@Nonnull Collection<?> c) {
        return compareAndRemove(true, c);
    }

    public boolean removeAll(@Nonnull Collection<?> c) {
        return compareAndRemove(false, c);
    }

    private boolean compareAndRemove(boolean retain, @Nonnull Collection<?> c) {
        checkNotNull(c, "Null collection is not allowed");

        Set<Data> valueSet = createHashSet(c.size());
        final NodeEngine nodeEngine = getNodeEngine();
        for (Object o : c) {
            checkNotNull(o, "Null collection element is not allowed");
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

    public <T> T[] toArray(@Nonnull T[] a) {
        checkNotNull(a, "Null array parameter is not allowed!");
        return getAll().toArray(a);
    }

    private Collection<E> getAll() {
        CollectionGetAllOperation operation = new CollectionGetAllOperation(name);
        SerializableList result = invoke(operation);
        List<Data> collection = result.getCollection();
        SerializationService serializationService = getNodeEngine().getSerializationService();
        return new UnmodifiableLazyList(collection, serializationService);
    }

    public @Nonnull
    UUID addItemListener(@Nonnull ItemListener<E> listener, boolean includeValue) {
        checkNotNull(listener, "Null listener is not allowed!");
        final EventService eventService = getNodeEngine().getEventService();
        final CollectionEventFilter filter = new CollectionEventFilter(includeValue);
        final EventRegistration registration = eventService.registerListener(getServiceName(), name, filter, listener);
        return registration.getId();
    }

    public boolean removeItemListener(@Nonnull UUID registrationId) {
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

    protected void checkIndexNotNegative(int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException("Index is negative");
        }
    }
}
