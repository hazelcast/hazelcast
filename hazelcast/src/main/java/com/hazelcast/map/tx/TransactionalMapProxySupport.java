/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.tx;

import com.hazelcast.instance.ThreadContext;
import com.hazelcast.map.ContainsKeyOperation;
import com.hazelcast.map.GetOperation;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.util.ExceptionUtil;

import java.util.ConcurrentModificationException;
import java.util.concurrent.Future;

/**
 * @mdogan 2/26/13
 */
public abstract class TransactionalMapProxySupport extends AbstractDistributedObject<MapService> implements TransactionalObject {

    protected final String name;
    protected final Transaction tx;

    public TransactionalMapProxySupport(String name, MapService mapService, NodeEngine nodeEngine, Transaction transaction) {
        super(nodeEngine, mapService);
        this.name = name;
        this.tx = transaction;
    }

    public boolean containsKeyInternal(Data key) {
        if (tx.getState() != Transaction.State.ACTIVE) {
            throw new IllegalStateException("Transaction is not active!");
        }
        ContainsKeyOperation operation = new ContainsKeyOperation(name, key);
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        try {
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(MapService.SERVICE_NAME, operation, partitionId).build();
            Future f = invocation.invoke();
            return (Boolean) f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Data getInternal(Data key) {
        if (tx.getState() != Transaction.State.ACTIVE) {
            throw new IllegalStateException("Transaction is not active!");
        }
        final MapService mapService = getService();
        final boolean nearCacheEnabled = mapService.getMapContainer(name).isNearCacheEnabled();
        if (nearCacheEnabled) {
            Data cachedData = mapService.getFromNearCache(name, key);
            if (cachedData != null)
                return cachedData;
        }
        GetOperation operation = new GetOperation(name, key);
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        try {
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(MapService.SERVICE_NAME, operation, partitionId).build();
            Future f = invocation.invoke();
            return (Data) f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Data putInternal(Data key, Data value) {
        VersionedValue versionedValue = lockAndGet(key, -1, 10000);
        if (versionedValue == null) {
            throw new ConcurrentModificationException("Transaction couldn't obtain lock");
        }
        tx.addTransactionLog(new MapTransactionLog(name, key, new TxnPutOperation(name, key, value, -1), versionedValue.version));
        return versionedValue.value;
    }

    private VersionedValue lockAndGet(Data key, long ttl, long timeout) {
        final NodeEngine nodeEngine = getNodeEngine();
        TxnLockAndGetOperation operation = new TxnLockAndGetOperation(name, key, ttl, timeout);
        operation.setThreadId(ThreadContext.getThreadId());
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(MapService.SERVICE_NAME, operation, partitionId).build();
            Future<VersionedValue> f = invocation.invoke();
            return f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }

    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }
}
