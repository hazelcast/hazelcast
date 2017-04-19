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

package com.hazelcast.map.impl.tx;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.TransactionalDistributedObject;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import com.hazelcast.transaction.TransactionTimedOutException;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.util.ThreadUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Base class contains proxy helper methods for {@link com.hazelcast.map.impl.tx.TransactionalMapProxy}
 */
public abstract class TransactionalMapProxySupport extends TransactionalDistributedObject<MapService> {

    protected final Map<Data, VersionedValue> valueMap = new HashMap<Data, VersionedValue>();

    protected final String name;
    protected final MapServiceContext mapServiceContext;
    protected final MapNearCacheManager mapNearCacheManager;
    protected final MapOperationProvider operationProvider;
    protected final PartitioningStrategy partitionStrategy;
    protected final IPartitionService partitionService;
    protected final OperationService operationService;

    private final RecordFactory recordFactory;
    private final boolean nearCacheEnabled;

    TransactionalMapProxySupport(String name, MapService mapService, NodeEngine nodeEngine, Transaction transaction) {
        super(nodeEngine, mapService, transaction);
        this.name = name;
        this.mapServiceContext = mapService.getMapServiceContext();
        this.mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        this.operationProvider = mapServiceContext.getMapOperationProvider(name);
        MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        this.partitionStrategy = mapContainer.getPartitioningStrategy();
        this.partitionService = nodeEngine.getPartitionService();
        this.operationService = nodeEngine.getOperationService();
        this.recordFactory = mapContainer.getRecordFactoryConstructor().createNew(null);
        this.nearCacheEnabled = mapContainer.getMapConfig().isNearCacheEnabled();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public final String getServiceName() {
        return SERVICE_NAME;
    }

    boolean isEquals(Object value1, Object value2) {
        return recordFactory.isEquals(value1, value2);
    }

    void checkTransactionState() {
        if (!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    boolean containsKeyInternal(Data key) {
        MapOperation operation = operationProvider.createContainsKeyOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        int partitionId = partitionService.getPartitionId(key);
        try {
            Future future = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
            return (Boolean) future.get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    Object getInternal(Data key) {
        if (nearCacheEnabled) {
            Object value = getCachedValue(key, true);
            if (value != NOT_CACHED) {
                return value;
            }
        }

        MapOperation operation = operationProvider.createGetOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        int partitionId = partitionService.getPartitionId(key);
        try {
            Future future = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
            return future.get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private Object getCachedValue(Data key, boolean deserializeValue) {
        Object value = mapNearCacheManager.getFromNearCache(name, key);

        if (value == null) {
            return NOT_CACHED;
        }

        if (value == CACHED_AS_NULL) {
            return null;
        }

        return deserializeValue ? getNodeEngine().getSerializationService().toObject(value) : value;
    }

    Object getForUpdateInternal(Data key) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis(), true);
        addUnlockTransactionRecord(key, versionedValue.version);
        return versionedValue.value;
    }

    int sizeInternal() {
        try {
            OperationFactory sizeOperationFactory = operationProvider.createMapSizeOperationFactory(name);
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(SERVICE_NAME, sizeOperationFactory);
            int total = 0;
            for (Object result : results.values()) {
                Integer size = getNodeEngine().toObject(result);
                total += size;
            }
            return total;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    Data putInternal(Data key, Data value, long ttl, TimeUnit timeUnit) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        long timeInMillis = getTimeInMillis(ttl, timeUnit);
        MapOperation operation = operationProvider.createTxnSetOperation(name, key, value, versionedValue.version, timeInMillis);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), operation, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    Data putIfAbsentInternal(Data key, Data value) {
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

    Data replaceInternal(Data key, Data value) {
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

    boolean replaceIfSameInternal(Data key, Object oldValue, Data newValue) {
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

    Data removeInternal(Data key) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key),
                operationProvider.createTxnDeleteOperation(name, key, versionedValue.version),
                versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    boolean removeIfSameInternal(Data key, Object value) {
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
            TxnUnlockOperation unlockOperation = new TxnUnlockOperation(name, key, versionedValue.version);
            unlockOperation.setThreadId(ThreadUtil.getThreadId());
            unlockOperation.setOwnerUuid(tx.getOwnerUuid());
            int partitionId = partitionService.getPartitionId(key);
            Future<VersionedValue> future = operationService.invokeOnPartition(SERVICE_NAME, unlockOperation, partitionId);
            future.get();
            valueMap.remove(key);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private void addUnlockTransactionRecord(Data key, long version) {
        TxnUnlockOperation operation = new TxnUnlockOperation(name, key, version);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), operation, version, tx.getOwnerUuid()));
    }

    /**
     * Locks the key on the partition owner and returns the value with the version. Does not invokes maploader if
     * the key is missing in memory
     *
     * @param key     serialized key
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
        boolean blockReads = tx.getTransactionType() == TransactionType.ONE_PHASE;
        MapOperation operation = operationProvider.createTxnLockAndGetOperation(name, key, timeout, timeout,
                tx.getOwnerUuid(), shouldLoad, blockReads);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            int partitionId = partitionService.getPartitionId(key);
            Future<VersionedValue> future = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
            versionedValue = future.get();
            if (versionedValue == null) {
                throw new TransactionTimedOutException("Transaction couldn't obtain lock for the key: " + toObjectIfNeeded(key));
            }
            valueMap.put(key, versionedValue);
            return versionedValue;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private static long getTimeInMillis(long time, TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }
}
