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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.ItemListener;
import com.hazelcast.collection.impl.queue.operations.AddAllOperation;
import com.hazelcast.collection.impl.queue.operations.ClearOperation;
import com.hazelcast.collection.impl.queue.operations.CompareAndRemoveOperation;
import com.hazelcast.collection.impl.queue.operations.ContainsOperation;
import com.hazelcast.collection.impl.queue.operations.DrainOperation;
import com.hazelcast.collection.impl.queue.operations.IsEmptyOperation;
import com.hazelcast.collection.impl.queue.operations.IteratorOperation;
import com.hazelcast.collection.impl.queue.operations.OfferOperation;
import com.hazelcast.collection.impl.queue.operations.PeekOperation;
import com.hazelcast.collection.impl.queue.operations.PollOperation;
import com.hazelcast.collection.impl.queue.operations.QueueOperation;
import com.hazelcast.collection.impl.queue.operations.RemainingCapacityOperation;
import com.hazelcast.collection.impl.queue.operations.RemoveOperation;
import com.hazelcast.collection.impl.queue.operations.SizeOperation;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.InitializingObject;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.SerializableList;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

abstract class QueueProxySupport<E> extends AbstractDistributedObject<QueueService> implements InitializingObject {

    final String name;
    final int partitionId;
    final QueueConfig config;

    QueueProxySupport(final String name, final QueueService queueService, NodeEngine nodeEngine, QueueConfig config) {
        super(nodeEngine, queueService);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
        this.config = config;
    }

    @Override
    public void initialize() {
        final NodeEngine nodeEngine = getNodeEngine();
        final List<ItemListenerConfig> itemListenerConfigs = config.getItemListenerConfigs();
        for (ItemListenerConfig itemListenerConfig : itemListenerConfigs) {
            ItemListener listener = itemListenerConfig.getImplementation();
            if (listener == null && itemListenerConfig.getClassName() != null) {
                try {
                    listener = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(),
                            itemListenerConfig.getClassName());
                } catch (Exception e) {
                    throw rethrow(e);
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

    public int getPartitionId() {
        return partitionId;
    }

    boolean offerInternal(Data data, long timeout) throws InterruptedException {
        checkObjectNotNull(data);

        OfferOperation operation = new OfferOperation(name, timeout, data);
        return (Boolean) invokeAndGet(operation, InterruptedException.class);
    }

    public boolean isEmpty() {
        IsEmptyOperation operation = new IsEmptyOperation(name);
        return (Boolean) invokeAndGet(operation);
    }

    public int size() {
        SizeOperation operation = new SizeOperation(name);
        return (Integer) invokeAndGet(operation);
    }

    public int remainingCapacity() {
        RemainingCapacityOperation operation = new RemainingCapacityOperation(name);
        return (Integer) invokeAndGet(operation);
    }

    public void clear() {
        ClearOperation operation = new ClearOperation(name);
        invokeAndGet(operation);
    }

    Object peekInternal() {
        PeekOperation operation = new PeekOperation(name);
        return invokeAndGetData(operation);
    }

    Object pollInternal(long timeout) throws InterruptedException {
        PollOperation operation = new PollOperation(name, timeout);
        return invokeAndGet(operation, InterruptedException.class);
    }

    boolean removeInternal(Data data) {
        checkObjectNotNull(data);

        RemoveOperation operation = new RemoveOperation(name, data);
        return (Boolean) invokeAndGet(operation);
    }

    boolean containsInternal(Collection<Data> dataList) {
        ContainsOperation operation = new ContainsOperation(name, dataList);
        return (Boolean) invokeAndGet(operation);
    }

    List<Data> listInternal() {
        IteratorOperation operation = new IteratorOperation(name);
        SerializableList collectionContainer = invokeAndGet(operation);
        return (List<Data>) collectionContainer.getCollection();
    }

    Collection<Data> drainInternal(int maxSize) {
        DrainOperation operation = new DrainOperation(name, maxSize);
        SerializableList collectionContainer = invokeAndGet(operation);
        return collectionContainer.getCollection();
    }

    boolean addAllInternal(Collection<Data> dataList) {
        AddAllOperation operation = new AddAllOperation(name, dataList);
        return (Boolean) invokeAndGet(operation);
    }

    boolean compareAndRemove(Collection<Data> dataList, boolean retain) {
        CompareAndRemoveOperation operation = new CompareAndRemoveOperation(name, dataList, retain);
        return (Boolean) invokeAndGet(operation);
    }

    protected void checkObjectNotNull(Object o) {
        checkNotNull(o, "Object is null");
    }

    private <T> T invokeAndGet(QueueOperation operation) {
        return invokeAndGet(operation, RuntimeException.class);
    }

    private <T, E extends Throwable> T invokeAndGet(QueueOperation operation, Class<E> allowedException) throws E {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Future f = invoke(operation);
            return (T) nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw rethrow(throwable, allowedException);
        }
    }

    private InvocationFuture<Object> invoke(Operation operation) {
        final NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        return operationService.invokeOnPartition(QueueService.SERVICE_NAME, operation, getPartitionId());
    }

    private Object invokeAndGetData(QueueOperation operation) {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            OperationService operationService = nodeEngine.getOperationService();
            Future f = operationService.invokeOnPartition(QueueService.SERVICE_NAME, operation, partitionId);
            return f.get();
        } catch (Throwable throwable) {
            throw rethrow(throwable);
        }
    }

    @Override
    public final String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Nonnull
    @Override
    public final String getName() {
        return name;
    }

    public @Nonnull
    UUID addItemListener(@Nonnull ItemListener<E> listener,
                           boolean includeValue) {
        checkNotNull(listener, "Null listener is not allowed!");
        return getService().addItemListener(name, listener, includeValue);
    }

    public boolean removeItemListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "Null registrationId is not allowed!");
        return getService().removeItemListener(name, registrationId);
    }
}
