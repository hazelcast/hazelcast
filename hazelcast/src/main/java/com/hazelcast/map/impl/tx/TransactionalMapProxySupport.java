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

package com.hazelcast.map.impl.tx;

import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.impl.CompositeRemoteCallHook;
import com.hazelcast.internal.nearcache.impl.RemoteCallHook;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.map.impl.InterceptorRegistry;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.TransactionalDistributedObject;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import com.hazelcast.transaction.TransactionTimedOutException;
import com.hazelcast.transaction.impl.Transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.map.impl.MapOperationStatsUpdater.incrementTxnOperationStats;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.record.Record.UNSET;

/**
 * Base class contains proxy helper methods for {@link com.hazelcast.map.impl.tx.TransactionalMapProxy}
 */
public abstract class TransactionalMapProxySupport extends TransactionalDistributedObject<MapService> {

    protected final Map<Data, VersionedValue> valueMap = new HashMap<>();

    protected final String name;
    protected final boolean statisticsEnabled;
    protected final MapServiceContext mapServiceContext;
    protected final MapNearCacheManager mapNearCacheManager;
    protected final MapOperationProvider operationProvider;
    protected final PartitioningStrategy partitionStrategy;
    protected final IPartitionService partitionService;
    protected final OperationService operationService;
    protected final InternalSerializationService ss;

    private final boolean serializeKeys;
    private final boolean nearCacheEnabled;
    private final ValueComparator valueComparator;
    private final LocalMapStatsImpl localMapStats;

    TransactionalMapProxySupport(String name, MapService mapService,
                                 NodeEngine nodeEngine, Transaction transaction) {
        super(nodeEngine, mapService, transaction);

        this.name = name;
        this.mapServiceContext = mapService.getMapServiceContext();
        this.mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        MapConfig mapConfig = nodeEngine.getConfig().findMapConfig(name);
        this.operationProvider = mapServiceContext.getMapOperationProvider(name);
        this.partitionStrategy = mapServiceContext.getPartitioningStrategy(name,
                mapConfig.getPartitioningStrategyConfig());
        this.partitionService = nodeEngine.getPartitionService();
        this.operationService = nodeEngine.getOperationService();
        this.ss = ((InternalSerializationService) nodeEngine.getSerializationService());
        this.nearCacheEnabled = mapConfig.isNearCacheEnabled();
        this.serializeKeys = nearCacheEnabled && mapConfig.getNearCacheConfig().isSerializeKeys();
        this.valueComparator = mapServiceContext.getValueComparatorOf(mapConfig.getInMemoryFormat());
        this.statisticsEnabled = mapConfig.isStatisticsEnabled();
        this.localMapStats = mapServiceContext.getLocalMapStatsProvider().getLocalMapStatsImpl(name);
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
        return valueComparator.isEqual(value1, value2, ss);
    }

    void checkTransactionState() {
        if (!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    boolean containsKeyInternal(Data dataKey, Object objectKey, boolean skipNearCacheLookup) {
        if (!skipNearCacheLookup && nearCacheEnabled) {
            Object nearCacheKey = serializeKeys ? dataKey : objectKey;
            Object cachedValue = getCachedValue(nearCacheKey, false);
            if (cachedValue != NOT_CACHED) {
                return cachedValue != null;
            }
        }

        MapOperation operation = operationProvider.createContainsKeyOperation(name, dataKey);
        operation.setThreadId(ThreadUtil.getThreadId());
        int partitionId = partitionService.getPartitionId(dataKey);
        try {
            Future future = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
            Object result = future.get();
            incrementOtherOperationsStat();
            return (Boolean) result;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    Object getInternal(Object nearCacheKey, Data keyData, boolean skipNearCacheLookup, long startNanos) {
        if (!skipNearCacheLookup && nearCacheEnabled) {
            Object value = getCachedValue(nearCacheKey, true);
            if (value != NOT_CACHED) {
                return value;
            }
        }

        MapOperation operation = operationProvider.createGetOperation(name, keyData);
        operation.setThreadId(ThreadUtil.getThreadId());
        int partitionId = partitionService.getPartitionId(keyData);
        try {
            Future future = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                    .setResultDeserialized(false)
                    .invoke();
            Object result = future.get();

            if (statisticsEnabled) {
                updateOpStats(operation, startNanos);
            }

            return result;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    final Object toNearCacheKeyWithStrategy(Object key) {
        if (!nearCacheEnabled) {
            return key;
        }

        return serializeKeys ? ss.toData(key, partitionStrategy) : key;
    }

    final void invalidateNearCache(Object nearCacheKey) {
        if (!nearCacheEnabled) {
            return;
        }
        if (nearCacheKey == null) {
            return;
        }
        NearCache<Object, Object> nearCache = mapNearCacheManager.getNearCache(name);
        if (nearCache == null) {
            return;
        }

        nearCache.invalidate(nearCacheKey);
    }

    private Object getCachedValue(Object nearCacheKey, boolean deserializeValue) {
        NearCache<Object, Object> nearCache = mapNearCacheManager.getNearCache(name);
        if (nearCache == null) {
            return NOT_CACHED;
        }

        Object value = nearCache.get(nearCacheKey);
        if (value == null) {
            return NOT_CACHED;
        }
        if (value == CACHED_AS_NULL) {
            return null;
        }

        InterceptorRegistry interceptorRegistry = mapServiceContext.getMapContainer(name).getInterceptorRegistry();
        mapServiceContext.interceptAfterGet(interceptorRegistry, value);
        return deserializeValue ? ss.toObject(value) : value;
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
            incrementOtherOperationsStat();
            return total;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    Data putInternal(Data key, Data value, long ttl, TimeUnit timeUnit, RemoteCallHook hook) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        long timeInMillis = getTimeInMillis(ttl, timeUnit);
        MapOperation op = operationProvider.createTxnSetOperation(name, key, value, versionedValue.version, timeInMillis);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), op, tx.getOwnerUuid(), tx.getTxnId(), hook));
        return versionedValue.value;
    }

    Data putIfAbsentInternal(Data key, Data value, RemoteCallHook hook) {
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

        MapOperation operation = operationProvider.createTxnSetOperation(name, key, value,
                versionedValue.version, UNSET);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key),
                operation, tx.getOwnerUuid(), tx.getTxnId(), hook));
        return versionedValue.value;
    }

    Data replaceInternal(Data key, Data value, RemoteCallHook hook) {
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
        MapOperation operation = operationProvider.createTxnSetOperation(name, key, value,
                versionedValue.version, UNSET);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), operation,
                tx.getOwnerUuid(), tx.getTxnId(), hook));
        return versionedValue.value;
    }

    boolean replaceIfSameInternal(Data key, Object oldValue,
                                  Data newValue, RemoteCallHook hook) {
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

        MapOperation operation = operationProvider.createTxnSetOperation(name, key, newValue,
                versionedValue.version, UNSET);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), operation,
                tx.getOwnerUuid(), tx.getTxnId(), hook));
        return true;
    }

    Data removeInternal(Data key, RemoteCallHook hook) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key),
                operationProvider.createTxnDeleteOperation(name, key, versionedValue.version),
                tx.getOwnerUuid(), tx.getTxnId(), hook));
        return versionedValue.value;
    }

    boolean removeIfSameInternal(Data key, Object value, RemoteCallHook hook) {
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
                tx.getOwnerUuid(), tx.getTxnId(), hook));
        return true;
    }

    private void unlock(Data key, VersionedValue versionedValue) {
        try {
            TxnUnlockOperation unlockOperation = new TxnUnlockOperation(name, key, versionedValue.version);
            unlockOperation.setThreadId(ThreadUtil.getThreadId());
            unlockOperation.setOwnerUuid(tx.getOwnerUuid());
            int partitionId = partitionService.getPartitionId(key);
            Future<VersionedValue> future
                    = operationService.invokeOnPartition(SERVICE_NAME, unlockOperation, partitionId);
            future.get();
            valueMap.remove(key);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private void addUnlockTransactionRecord(Data key, long version) {
        TxnUnlockOperation operation = new TxnUnlockOperation(name, key, version);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key),
                operation, tx.getOwnerUuid(), tx.getTxnId(), RemoteCallHook.EMPTY_HOOK));
    }

    /**
     * Locks the key on the partition owner and returns
     * the value with the version. Does not invokes
     * maploader if the key is missing in memory.
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

    protected RemoteCallHook newRemoteCallHook() {
        CompositeRemoteCallHook hook = null;

        if (nearCacheEnabled) {
            hook = new CompositeRemoteCallHook();
            hook.add(new InvalidationHook());
        }

        if (statisticsEnabled) {
            hook = hook != null ? hook : new CompositeRemoteCallHook();
            hook.add(new StatsUpdaterHook());
        }

        return hook != null ? hook : RemoteCallHook.EMPTY_HOOK;
    }

    /**
     * Hook to be used by local map stats updates.
     */
    private class StatsUpdaterHook implements RemoteCallHook {

        private long startNanos;

        @Override
        public void beforeRemoteCall(Object key, Data keyData,
                                     Object value, Data valueData) {
            startNanos = Timer.nanos();
        }

        @Override
        public void onRemoteCallSuccess(Operation remoteCall) {
            updateOpStats(remoteCall, startNanos);
        }

        @Override
        public void onRemoteCallFailure() {
        }
    }

    protected void updateOpStats(Operation op, long startNanos) {
        assert statisticsEnabled;

        incrementTxnOperationStats(op, localMapStats, startNanos);
    }

    protected void incrementOtherOperationsStat() {
        if (!statisticsEnabled) {
            return;
        }

        localMapStats.incrementOtherOperations();
    }

    /**
     * Hook for near cahe invalidations.
     */
    private class InvalidationHook implements RemoteCallHook {

        private Object nearCacheKey;

        @Override
        public void beforeRemoteCall(Object key, Data keyData, Object value, Data valueData) {
            nearCacheKey = serializeKeys ? keyData : mapServiceContext.toObject(key);
        }

        @Override
        public void onRemoteCallSuccess(Operation remoteCall) {
            invalidateNearCache(nearCacheKey);
        }

        @Override
        public void onRemoteCallFailure() {
            invalidateNearCache(nearCacheKey);
        }
    }
}
