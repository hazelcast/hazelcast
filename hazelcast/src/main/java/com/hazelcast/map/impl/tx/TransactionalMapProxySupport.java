/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.tx;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.nearcache.NearCache.NULL_OBJECT;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Base class contains proxy helper methods for {@link com.hazelcast.map.impl.tx.TransactionalMapProxy}
 */
public abstract class TransactionalMapProxySupport extends AbstractDistributedObject<MapService> implements TransactionalObject {

    protected final String name;
    protected final Transaction tx;
    protected final PartitioningStrategy partitionStrategy;
    protected final Map<Data, VersionedValue> valueMap = new HashMap<Data, VersionedValue>();
    protected final RecordFactory recordFactory;
    protected final MapOperationProvider operationProvider;
    protected final MapServiceContext mapServiceContext;

    public TransactionalMapProxySupport(String name, MapService mapService, NodeEngine nodeEngine, Transaction transaction) {
        super(nodeEngine, mapService);
        this.name = name;
        this.tx = transaction;
        this.mapServiceContext = mapService.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        this.recordFactory = mapContainer.getRecordFactoryConstructor().createNew(null);
        this.operationProvider = mapServiceContext.getMapOperationProvider(name);
        this.partitionStrategy = mapContainer.getPartitioningStrategy();
    }

    protected boolean isEquals(Object value1, Object value2) {
        return recordFactory.isEquals(value1, value2);
    }

    protected void checkTransactionState() {
        if (!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    public boolean containsKeyInternal(Data key) {
        MapOperation operation = operationProvider.createContainsKeyOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        try {
            Future future = nodeEngine.getOperationService().invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
            return (Boolean) future.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Object getInternal(Data key) {
        MapService mapService = getService();
        boolean nearCacheEnabled = mapService.getMapServiceContext().getMapContainer(name).isNearCacheEnabled();
        if (nearCacheEnabled) {
            Object cached = mapService.getMapServiceContext().getNearCacheProvider().getFromNearCache(name, key);
            if (cached != null) {
                if (cached.equals(NULL_OBJECT)) {
                    cached = null;
                }
                return cached;
            }
        }
        MapOperation operation = operationProvider.createGetOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        try {
            Future future = nodeEngine.getOperationService()
                    .invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
            return future.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Object getForUpdateInternal(Data key) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis(), true);
        addUnlockTransactionRecord(key, versionedValue.version);
        return versionedValue.value;
    }

    public int sizeInternal() {
        NodeEngine nodeEngine = getNodeEngine();
        try {
            OperationFactory sizeOperationFactory = operationProvider.createMapSizeOperationFactory(name);
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, sizeOperationFactory);
            int total = 0;
            for (Object result : results.values()) {
                Integer size = (Integer) getService().getMapServiceContext().toObject(result);
                total += size;
            }
            return total;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Data putInternal(Data key, Data value) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        MapOperation operation = operationProvider.createTxnSetOperation(name, key, value, versionedValue.version, -1);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), operation, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public Data putInternal(Data key, Data value, long ttl, TimeUnit timeUnit) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        long timeInMillis = getTimeInMillis(ttl, timeUnit);
        MapOperation operation = operationProvider.createTxnSetOperation(name, key, value, versionedValue.version, timeInMillis);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key),
                operation, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public Data putIfAbsentInternal(Data key, Data value) {
        boolean unlockImmediately = !valueMap.containsKey(key);
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (versionedValue.value != null) {
            if (unlockImmediately) {
                unlock(key, versionedValue);
                return versionedValue.value;
            }
            addUnlockTransactionRecord(key, versionedValue.version);
            return versionedValue.value;
        }

        MapOperation operation = operationProvider.createTxnSetOperation(name, key, value, versionedValue.version, -1);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), operation, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public Data replaceInternal(Data key, Data value) {
        boolean unlockImmediately = !valueMap.containsKey(key);
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (versionedValue.value == null) {
            if (unlockImmediately) {
                unlock(key, versionedValue);
                return null;
            }
            addUnlockTransactionRecord(key, versionedValue.version);
            return null;
        }
        MapOperation operation = operationProvider.createTxnSetOperation(name, key, value, versionedValue.version, -1);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), operation, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public boolean replaceIfSameInternal(Data key, Object oldValue, Data newValue) {
        boolean unlockImmediately = !valueMap.containsKey(key);
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (!isEquals(oldValue, versionedValue.value)) {
            if (unlockImmediately) {
                unlock(key, versionedValue);
                return false;
            }
            addUnlockTransactionRecord(key, versionedValue.version);
            return false;
        }
        MapOperation operation = operationProvider.createTxnSetOperation(name, key, newValue, versionedValue.version, -1);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), operation, versionedValue.version, tx.getOwnerUuid()));
        return true;
    }

    public Data removeInternal(Data key) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key),
                operationProvider.createTxnDeleteOperation(name, key, versionedValue.version),
                versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public boolean removeIfSameInternal(Data key, Object value) {
        boolean unlockImmediately = !valueMap.containsKey(key);
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (!isEquals(versionedValue.value, value)) {
            if (unlockImmediately) {
                unlock(key, versionedValue);
                return false;
            }
            addUnlockTransactionRecord(key, versionedValue.version);
            return false;
        }
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key),
                operationProvider.createTxnDeleteOperation(name, key, versionedValue.version),
                versionedValue.version, tx.getOwnerUuid()));
        return true;
    }


    private void unlock(Data key, VersionedValue versionedValue) {
        try {
            NodeEngine nodeEngine = getNodeEngine();
            TxnUnlockOperation unlockOperation = new TxnUnlockOperation(name, key, versionedValue.version);
            unlockOperation.setThreadId(ThreadUtil.getThreadId());
            unlockOperation.setOwnerUuid(tx.getOwnerUuid());
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Future<VersionedValue> future = nodeEngine.getOperationService()
                    .invokeOnPartition(MapService.SERVICE_NAME, unlockOperation, partitionId);
            future.get();
            valueMap.remove(key);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private void addUnlockTransactionRecord(Data key, long version) {
        TxnUnlockOperation operation = new TxnUnlockOperation(name, key, version);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), operation, version, tx.getOwnerUuid()));
    }

    /**
     * Locks the key on the partition owner and returns the value with the version. Does not invokes maploader if
     * the key is missing in memory
     * @param key serialized key
     * @param timeout timeout in millis
     * @return VersionedValue wrapper for value/version pair.
     */
    private VersionedValue lockAndGet(Data key, long timeout) {
        return lockAndGet(key, timeout, false);
    }

    private VersionedValue lockAndGet(Data key, long timeout, boolean shouldLoad) {
        VersionedValue versionedValue = valueMap.get(key);
        if (versionedValue != null) {
            return versionedValue;
        }
        NodeEngine nodeEngine = getNodeEngine();
        MapOperation operation = operationProvider.createTxnLockAndGetOperation(name, key, timeout, timeout,
                tx.getOwnerUuid(), shouldLoad);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Future<VersionedValue> future = nodeEngine.getOperationService()
                    .invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
            versionedValue = future.get();
            if (versionedValue == null) {
                throw new TransactionException("Transaction couldn't obtain lock for the key: "
                        + getService().getMapServiceContext().toObject(key));
            }
            valueMap.put(key, versionedValue);
            return versionedValue;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }

    }

    protected long getTimeInMillis(long time, TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    public String getName() {
        return name;
    }

    @Override
    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }
}
