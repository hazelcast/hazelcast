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
import com.hazelcast.nio.Data;
import com.hazelcast.queue.*;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;

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
        try {
            OfferOperation operation = new OfferOperation(name, timeout, data);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future f = inv.invoke();
            return (Boolean) nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            throw new RuntimeException(throwable);
        }
    }

    public int size() {
        try {
            SizeOperation operation = new SizeOperation(name);
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future future = invocation.invoke();
            Object result = future.get();
            return (Integer) nodeEngine.toObject(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public void clear() {
        try {
            ClearOperation operation = new ClearOperation(name);
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future future = invocation.invoke();
            future.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Data peekInternal() {
        try {
            PeekOperation operation = new PeekOperation(name);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future<Data> f = inv.invoke();
            return f.get();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    Data pollInternal(long timeout) {
        try {
            PollOperation operation = new PollOperation(name, timeout);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future<Data> f = inv.invoke();
            return f.get();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            throw new RuntimeException(throwable);
        }
    }

    boolean removeInternal(Data data) {
        checkNull(data);
        try {
            RemoveOperation operation = new RemoveOperation(name, data);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future f = inv.invoke();
            return (Boolean) nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    boolean containsInternal(List<Data> dataList) {
        try {
            ContainsOperation operation = new ContainsOperation(name, dataList);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future f = inv.invoke();
            return (Boolean) nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    List<Data> listInternal(){
        try {
            IteratorOperation operation = new IteratorOperation(name);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future f = inv.invoke();
            return (List<Data>)nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    List<Data> drainInternal(int maxSize){
        try {
            DrainOperation operation = new DrainOperation(name, maxSize);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future f = inv.invoke();
            return (List<Data>)nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    boolean addAllInternal(List<Data> dataSet){
        try {
            AddAllOperation operation = new AddAllOperation(name, dataSet);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future f = inv.invoke();
            return (Boolean) nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    boolean compareCollectionInternal(List<Data> dataSet, boolean retain){
        try {
            CompareAndRemoveOperation operation = new CompareAndRemoveOperation(name, dataSet, retain);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future f = inv.invoke();
            return (Boolean) nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }


    private int getPartitionId() {
        return partitionId;
    }

    private void checkNull(Data data) {
        if (data == null) {
            throw new NullPointerException();
        }
    }


}
