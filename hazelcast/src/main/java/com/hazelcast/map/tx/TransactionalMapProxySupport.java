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

import com.hazelcast.map.MapService;
import com.hazelcast.map.operation.ContainsKeyOperation;
import com.hazelcast.map.operation.GetOperation;
import com.hazelcast.map.operation.SizeOperationFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;

import java.util.Map;
import java.util.concurrent.Future;

import static com.hazelcast.map.MapService.SERVICE_NAME;

/**
 * @author mdogan 2/26/13
 */
public abstract class TransactionalMapProxySupport extends AbstractDistributedObject<MapService> implements TransactionalObject {

    protected final String name;
    protected final TransactionSupport tx;

    public TransactionalMapProxySupport(String name, MapService mapService, NodeEngine nodeEngine, TransactionSupport transaction) {
        super(nodeEngine, mapService);
        this.name = name;
        this.tx = transaction;
    }

    protected void checkTransactionState() {
        if (!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    public boolean containsKeyInternal(Data key) {
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

    public Object getInternal(Data key) {
        final MapService mapService = getService();
        final boolean nearCacheEnabled = mapService.getMapContainer(name).isNearCacheEnabled();
        if (nearCacheEnabled) {
            Object cached = mapService.getFromNearCache(name, key);
            if (cached != null)
                return cached;
        }
        GetOperation operation = new GetOperation(name, key);
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        try {
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(MapService.SERVICE_NAME, operation, partitionId).build();
            Future f = invocation.invoke();
            return f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }


    public int sizeInternal() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new SizeOperationFactory(name));
            int total = 0;
            for (Object result : results.values()) {
                Integer size = (Integer) getService().toObject(result);
                total += size;
            }
            return total;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Data putInternal(Data key, Data value) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (versionedValue == null) {
            throw new TransactionException("Transaction couldn't obtain lock!");
        }
        tx.addTransactionLog(new MapTransactionLog(name, key, new TxnSetOperation(name, key, value, -1, versionedValue.version), versionedValue.version));
        return versionedValue.value;
    }

    public Data putIfAbsentInternal(Data key, Data value) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (versionedValue == null) {
            throw new TransactionException("Transaction couldn't obtain lock!");
        }
        if (versionedValue.value != null)
            return versionedValue.value;

        tx.addTransactionLog(new MapTransactionLog(name, key, new TxnSetOperation(name, key, value, -1, versionedValue.version), versionedValue.version));
        return versionedValue.value;
    }

    public Data replaceInternal(Data key, Data value) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (versionedValue == null) {
            throw new TransactionException("Transaction couldn't obtain lock!");
        }
        if (versionedValue.value == null)
            return null;
        tx.addTransactionLog(new MapTransactionLog(name, key, new TxnSetOperation(name, key, value, -1, versionedValue.version), versionedValue.version));
        return versionedValue.value;
    }

    public boolean replaceIfSameInternal(Data key, Object oldValue, Data newValue) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (versionedValue == null) {
            throw new TransactionException("Transaction couldn't obtain lock!");
        }
        if (!getService().compare(name, oldValue, versionedValue.value))
            return false;
        tx.addTransactionLog(new MapTransactionLog(name, key, new TxnSetOperation(name, key, newValue, -1, versionedValue.version), versionedValue.version));
        return true;
    }

    public Data removeInternal(Data key) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (versionedValue == null) {
            throw new TransactionException("Transaction couldn't obtain lock!");
        }
        tx.addTransactionLog(new MapTransactionLog(name, key, new TxnDeleteOperation(name, key, versionedValue.version), versionedValue.version));
        return versionedValue.value;
    }

    public boolean removeIfSameInternal(Data key, Object value) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (versionedValue == null) {
            throw new TransactionException("Transaction couldn't obtain lock!");
        }
        if (!getService().compare(name, versionedValue.value, value)) {
            return false;
        }
        tx.addTransactionLog(new MapTransactionLog(name, key, new TxnDeleteOperation(name, key, versionedValue.version), versionedValue.version));
        return true;
    }

    private VersionedValue lockAndGet(Data key, long timeout) {
        final NodeEngine nodeEngine = getNodeEngine();
        TxnLockAndGetOperation operation = new TxnLockAndGetOperation(name, key, timeout, timeout);
        operation.setThreadId(ThreadUtil.getThreadId());
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
