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

package com.hazelcast.queue.proxy;

import com.hazelcast.config.QueueConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.*;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

/**
 * User: ali
 * Date: 11/14/12
 * Time: 12:47 AM
 */
abstract class QueueProxySupport {

    final String name;
    final QueueService queueService;
    final NodeEngine nodeEngine;
    final int partitionId;
    final QueueConfig config;

    QueueProxySupport(final String name, final QueueService queueService, NodeEngine nodeEngine) {
        this.name = name;
        this.queueService = queueService;
        this.nodeEngine = nodeEngine;
        this.partitionId = nodeEngine.getPartitionId(nodeEngine.toData(name));
        this.config = nodeEngine.getConfig().getQueueConfig(name);
    }

    boolean offerInternal(Data data, long timeout) {
        checkNull(data);
        OfferOperation operation = new OfferOperation(name, timeout, data);
        return (Boolean) invoke(operation);
    }

    public int size() {
        SizeOperation operation = new SizeOperation(name);
        return (Integer) invoke(operation);
    }

    public void clear() {
        ClearOperation operation = new ClearOperation(name);
        invoke(operation);
    }

    Data peekInternal() {
        PeekOperation operation = new PeekOperation(name);
        return invokeData(operation);
    }

    Data pollInternal(long timeout) {
        PollOperation operation = new PollOperation(name, timeout);
        return invokeData(operation);
    }

    boolean removeInternal(Data data) {
        checkNull(data);
        RemoveOperation operation = new RemoveOperation(name, data);
        return (Boolean) invoke(operation);
    }

    boolean containsInternal(Collection<Data> dataList) {
        ContainsOperation operation = new ContainsOperation(name, dataList);
        return (Boolean) invoke(operation);
    }

    List<Data> listInternal() {
        IteratorOperation operation = new IteratorOperation(name);
        SerializableCollectionContainer collectionContainer = invoke(operation);
        return (List<Data>)collectionContainer.getCollection();
    }

    List<Data> drainInternal(int maxSize) {
        DrainOperation operation = new DrainOperation(name, maxSize);
        SerializableCollectionContainer collectionContainer = invoke(operation);
        return (List<Data>)collectionContainer.getCollection();
    }

    boolean addAllInternal(Collection<Data> dataList) {
        AddAllOperation operation = new AddAllOperation(name, dataList);
        return (Boolean) invoke(operation);
    }

    boolean compareAndRemove(Collection<Data> dataList, boolean retain) {
        CompareAndRemoveOperation operation = new CompareAndRemoveOperation(name, dataList, retain);
        return (Boolean) invoke(operation);
    }


    private int getPartitionId() {
        return partitionId;
    }

    private void checkNull(Data data) {
        if (data == null) {
            throw new NullPointerException();
        }
    }

    public void destroy() {
        //TODO
    }

    private <T> T invoke(QueueOperation operation) {
        try {
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(QueueService.QUEUE_SERVICE_NAME, operation, getPartitionId()).build();
            Future f = inv.invoke();
            return (T) nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    private Data invokeData(QueueOperation operation) {
        try {
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(QueueService.QUEUE_SERVICE_NAME, operation, getPartitionId()).build();
            Future<Data> f = inv.invoke();
            return f.get();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }


}
