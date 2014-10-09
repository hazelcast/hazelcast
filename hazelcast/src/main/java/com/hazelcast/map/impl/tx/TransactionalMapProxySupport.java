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

package com.hazelcast.map.impl.tx;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.map.impl.MapKeySet;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.NearCache;
import com.hazelcast.map.impl.QueryResult;
import com.hazelcast.map.impl.operation.ContainsKeyOperation;
import com.hazelcast.map.impl.operation.GetOperation;
import com.hazelcast.map.impl.operation.MapEntrySetOperation;
import com.hazelcast.map.impl.operation.MapKeySetOperation;
import com.hazelcast.map.impl.operation.QueryOperation;
import com.hazelcast.map.impl.operation.QueryPartitionOperation;
import com.hazelcast.map.impl.operation.SizeOperationFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.BinaryOperationFactory;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.QueryResultSet;
import com.hazelcast.util.ThreadUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Base class contains proxy helper methods for {@link com.hazelcast.map.impl.tx.TransactionalMapProxy}
 */
public abstract class TransactionalMapProxySupport extends AbstractDistributedObject<MapService>
        implements TransactionalObject {

    protected final String name;
    protected final TransactionSupport tx;
    protected final PartitioningStrategy partitionStrategy;
    protected final Map<Data, VersionedValue> valueMap = new HashMap<Data, VersionedValue>();

    public TransactionalMapProxySupport(String name, MapService mapService, NodeEngine nodeEngine,
                                        TransactionSupport transaction) {
        super(nodeEngine, mapService);
        this.name = name;
        this.tx = transaction;
        this.partitionStrategy = mapService.getMapServiceContext().getMapContainer(name).getPartitioningStrategy();
    }

    protected void checkTransactionState() {
        if (!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    public boolean containsKeyInternal(Data key) {
        ContainsKeyOperation operation = new ContainsKeyOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        try {
            Future f = nodeEngine.getOperationService()
                    .invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
            return (Boolean) f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Object getInternal(Data key) {
        final MapService mapService = getService();
        final boolean nearCacheEnabled = mapService.getMapServiceContext().getMapContainer(name).isNearCacheEnabled();
        if (nearCacheEnabled) {
            Object cached = mapService.getMapServiceContext().getNearCacheProvider().getFromNearCache(name, key);
            if (cached != null) {
                if (cached.equals(NearCache.NULL_OBJECT)) {
                    cached = null;
                }
                return cached;
            }
        }
        GetOperation operation = new GetOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        try {
            Future f = nodeEngine.getOperationService()
                    .invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
            return f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Object getForUpdateInternal(Data key) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        addUnlockTransactionLog(key, versionedValue.version);
        return versionedValue.value;
    }

    public int sizeInternal() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new SizeOperationFactory(name));
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
        final TxnSetOperation op = new TxnSetOperation(name, key, value, versionedValue.version);
        tx.addTransactionLog(new MapTransactionLog(name, key, op, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public Data putInternal(Data key, Data value, long ttl, TimeUnit timeUnit) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        final long timeInMillis = getTimeInMillis(ttl, timeUnit);
        final TxnSetOperation op = new TxnSetOperation(name, key, value, versionedValue.version, timeInMillis);
        tx.addTransactionLog(new MapTransactionLog(name, key, op, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public Data putIfAbsentInternal(Data key, Data value) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (versionedValue.value != null) {
            addUnlockTransactionLog(key, versionedValue.version);
            return versionedValue.value;
        }

        final TxnSetOperation op = new TxnSetOperation(name, key, value, versionedValue.version);
        tx.addTransactionLog(new MapTransactionLog(name, key, op, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public Data replaceInternal(Data key, Data value) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (versionedValue.value == null) {
            addUnlockTransactionLog(key, versionedValue.version);
            return null;
        }
        final TxnSetOperation op = new TxnSetOperation(name, key, value, versionedValue.version);
        tx.addTransactionLog(new MapTransactionLog(name, key, op, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public boolean replaceIfSameInternal(Data key, Object oldValue, Data newValue) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (!getService().getMapServiceContext().compare(name, oldValue, versionedValue.value)) {
            addUnlockTransactionLog(key, versionedValue.version);
            return false;
        }
        final TxnSetOperation op = new TxnSetOperation(name, key, newValue, versionedValue.version);
        tx.addTransactionLog(new MapTransactionLog(name, key, op, versionedValue.version, tx.getOwnerUuid()));
        return true;
    }

    public Data removeInternal(Data key) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        tx.addTransactionLog(new MapTransactionLog(name, key,
                new TxnDeleteOperation(name, key, versionedValue.version), versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public boolean removeIfSameInternal(Data key, Object value) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (!getService().getMapServiceContext().compare(name, versionedValue.value, value)) {
            addUnlockTransactionLog(key, versionedValue.version);
            return false;
        }
        tx.addTransactionLog(new MapTransactionLog(name, key,
                new TxnDeleteOperation(name, key, versionedValue.version), versionedValue.version, tx.getOwnerUuid()));
        return true;
    }

    private void addUnlockTransactionLog(Data key, long version) {
        TxnUnlockOperation operation = new TxnUnlockOperation(name, key, version);
        tx.addTransactionLog(new MapTransactionLog(name, key, operation, version, tx.getOwnerUuid()));
    }

    private VersionedValue lockAndGet(Data key, long timeout) {
        VersionedValue versionedValue = valueMap.get(key);
        if (versionedValue != null) {
            return versionedValue;
        }
        final NodeEngine nodeEngine = getNodeEngine();
        TxnLockAndGetOperation operation
                = new TxnLockAndGetOperation(name, key, timeout, timeout, tx.getOwnerUuid());
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Future<VersionedValue> f = nodeEngine.getOperationService()
                    .invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
            versionedValue = f.get();
            if (versionedValue == null) {
                throw new TransactionException("Transaction couldn't obtain lock for the key:"
                        + getService().getMapServiceContext().toObject(key));
            }
            valueMap.put(key, versionedValue);
            return versionedValue;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected Set<Data> keySetInternal() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(
                            SERVICE_NAME,
                            new BinaryOperationFactory(new MapKeySetOperation(name), nodeEngine)
                    );
            Set<Data> keySet = new HashSet<Data>();
            for (Object result : results.values()) {
                Set keys = ((MapKeySet) getService().getMapServiceContext().toObject(result)).getKeySet();
                keySet.addAll(keys);
            }
            return keySet;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected List<Map.Entry<Data, Data>> getEntries() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(
                            SERVICE_NAME,
                            new BinaryOperationFactory(new MapEntrySetOperation(name), nodeEngine)
                    );
            List<Map.Entry<Data, Data>> entries = new ArrayList<Map.Entry<Data, Data>>();
            for (Object result : results.values()) {
                entries.addAll(((MapEntrySet) getService()
                        .getMapServiceContext().toObject(result)).getEntrySet());
            }
            return entries;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected Set queryInternal(final Predicate predicate, final IterationType iterationType, final boolean dataResult) {
        final NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        Set<Integer> partitions = new HashSet<Integer>(partitionCount);
        QueryResultSet result = new QueryResultSet(nodeEngine.getSerializationService(), iterationType, dataResult);
        List<Integer> missingList = new ArrayList<Integer>();
        try {
            List<Future> futures = new ArrayList<Future>();
            invokeQueryOperaration(predicate, operationService, members, futures);
            collectResults(partitions, result, futures);
            if (partitions.size() == partitionCount) {
                return result;
            }
            findMissingPartitions(partitionCount, partitions, missingList);
        } catch (Throwable t) {
            missingList.clear();
            findMissingPartitions(partitionCount, partitions, missingList);
        }
        try {
            List<Future> missingFutures = new ArrayList<Future>(missingList.size());
            invokeOperationOnMissingPartitions(predicate, operationService, missingList, missingFutures);
            collectResultsFromMissingPartitions(result, missingFutures);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return result;
    }

    private void collectResultsFromMissingPartitions(QueryResultSet result, List<Future> futures)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            result.addAll(queryResult.getResult());
        }
    }

    private void invokeOperationOnMissingPartitions(Predicate predicate, OperationService operationService,
                                                    List<Integer> missingList, List<Future> futures) {
        for (Integer pid : missingList) {
            QueryPartitionOperation queryPartitionOperation = new QueryPartitionOperation(name, predicate);
            queryPartitionOperation.setPartitionId(pid);
            try {
                Future f = operationService.invokeOnPartition(SERVICE_NAME, queryPartitionOperation, pid);
                futures.add(f);
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }

    private void findMissingPartitions(int partitionCount, Set<Integer> plist, List<Integer> missingList) {
        for (int i = 0; i < partitionCount; i++) {
            if (!plist.contains(i)) {
                missingList.add(i);
            }
        }
    }

    private void collectResults(Set<Integer> plist, QueryResultSet result, List<Future> futures)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            if (queryResult != null) {
                final Collection<Integer> partitionIds = queryResult.getPartitionIds();
                if (partitionIds != null) {
                    plist.addAll(partitionIds);
                    result.addAll(queryResult.getResult());
                }
            }
        }
    }

    private void invokeQueryOperaration(Predicate predicate, OperationService operationService,
                                        Collection<MemberImpl> members, List<Future> futures) {
        for (MemberImpl member : members) {
            Future future = operationService
                    .invokeOnTarget(SERVICE_NAME, new QueryOperation(name, predicate), member.getAddress());
            futures.add(future);
        }
    }

    protected long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    public String getName() {
        return name;
    }

    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }
}
