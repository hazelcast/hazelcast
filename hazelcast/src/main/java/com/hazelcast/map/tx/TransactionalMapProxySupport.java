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

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.MapKeySet;
import com.hazelcast.map.MapService;
import com.hazelcast.map.MapValueCollection;
import com.hazelcast.map.NearCache;
import com.hazelcast.map.QueryResult;
import com.hazelcast.map.operation.ContainsKeyOperation;
import com.hazelcast.map.operation.GetOperation;
import com.hazelcast.map.operation.MapKeySetOperation;
import com.hazelcast.map.operation.MapValuesOperation;
import com.hazelcast.map.operation.QueryOperation;
import com.hazelcast.map.operation.QueryPartitionOperation;
import com.hazelcast.map.operation.SizeOperationFactory;
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

import static com.hazelcast.map.MapService.SERVICE_NAME;

/**
 * @author mdogan 2/26/13
 */
public abstract class TransactionalMapProxySupport extends AbstractDistributedObject<MapService> implements TransactionalObject {

    protected final String name;
    protected final TransactionSupport tx;
    protected final PartitioningStrategy partitionStrategy;
    protected final Map<Data, VersionedValue> valueMap = new HashMap<Data, VersionedValue>();

    public TransactionalMapProxySupport(String name, MapService mapService, NodeEngine nodeEngine, TransactionSupport transaction) {
        super(nodeEngine, mapService);
        this.name = name;
        this.tx = transaction;
        this.partitionStrategy = mapService.getMapContainer(name).getPartitioningStrategy();
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
            Future f = nodeEngine.getOperationService().invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
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
            if (cached != null) {
                if (cached.equals(NearCache.NULL_OBJECT)) {
                    cached = null;
                }
                return cached;
            }
        }
        GetOperation operation = new GetOperation(name, key);
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        try {
            Future f = nodeEngine.getOperationService().invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
            return f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Object getForUpdateInternal(Data key) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        TxnUnlockOperation operation = new TxnUnlockOperation(name, key, versionedValue.version);
        tx.addTransactionLog(new MapTransactionLog(name, key, operation, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
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
            return versionedValue.value;
        }

        final TxnSetOperation op = new TxnSetOperation(name, key, value, versionedValue.version);
        tx.addTransactionLog(new MapTransactionLog(name, key, op, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public Data replaceInternal(Data key, Data value) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (versionedValue.value == null) {
            return null;
        }
        final TxnSetOperation op = new TxnSetOperation(name, key, value, versionedValue.version);
        tx.addTransactionLog(new MapTransactionLog(name, key, op, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public boolean replaceIfSameInternal(Data key, Object oldValue, Data newValue) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (!getService().compare(name, oldValue, versionedValue.value)) {
            return false;
        }
        final TxnSetOperation op = new TxnSetOperation(name, key, newValue, versionedValue.version);
        tx.addTransactionLog(new MapTransactionLog(name, key, op, versionedValue.version, tx.getOwnerUuid()));
        return true;
    }

    public Data removeInternal(Data key) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        tx.addTransactionLog(new MapTransactionLog(name, key, new TxnDeleteOperation(name, key, versionedValue.version), versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public boolean removeIfSameInternal(Data key, Object value) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (!getService().compare(name, versionedValue.value, value)) {
            return false;
        }
        tx.addTransactionLog(new MapTransactionLog(name, key, new TxnDeleteOperation(name, key, versionedValue.version), versionedValue.version, tx.getOwnerUuid()));
        return true;
    }

    private VersionedValue lockAndGet(Data key, long timeout) {
        VersionedValue versionedValue = valueMap.get(key);
        if (versionedValue != null) {
            return versionedValue;
        }
        final NodeEngine nodeEngine = getNodeEngine();
        TxnLockAndGetOperation operation = new TxnLockAndGetOperation(name, key, timeout, timeout, tx.getOwnerUuid());
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Future<VersionedValue> f = nodeEngine.getOperationService().invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
            versionedValue = f.get();
            if (versionedValue == null) {
                throw new TransactionException("Transaction couldn't obtain lock for the key:" + getService().toObject(key));
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
                    .invokeOnAllPartitions(SERVICE_NAME, new BinaryOperationFactory(new MapKeySetOperation(name), nodeEngine));
            Set<Data> keySet = new HashSet<Data>();
            for (Object result : results.values()) {
                Set keys = ((MapKeySet) getService().toObject(result)).getKeySet();
                keySet.addAll(keys);
            }
            return keySet;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected Collection<Data> valuesInternal() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new BinaryOperationFactory(new MapValuesOperation(name), nodeEngine));
            List<Data> values = new ArrayList<Data>();
            for (Object result : results.values()) {
                values.addAll(((MapValueCollection) getService().toObject(result)).getValues());
            }
            return values;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected Set queryInternal(final Predicate predicate, final IterationType iterationType, final boolean dataResult) {
        final NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        Set<Integer> plist = new HashSet<Integer>(partitionCount);
        QueryResultSet result = new QueryResultSet(nodeEngine.getSerializationService(), iterationType, dataResult);
        List<Integer> missingList = new ArrayList<Integer>();
        try {
            List<Future> flist = new ArrayList<Future>();
            for (MemberImpl member : members) {
                Future future = operationService
                        .invokeOnTarget(SERVICE_NAME, new QueryOperation(name, predicate), member.getAddress());
                flist.add(future);
            }
            for (Future future : flist) {
                QueryResult queryResult = (QueryResult) future.get();
                if (queryResult != null) {
                    final List<Integer> partitionIds = queryResult.getPartitionIds();
                    if (partitionIds != null) {
                        plist.addAll(partitionIds);
                        result.addAll(queryResult.getResult());
                    }
                }
            }
            if (plist.size() == partitionCount) {
                return result;
            }
            for (int i = 0; i < partitionCount; i++) {
                if (!plist.contains(i)) {
                    missingList.add(i);
                }
            }
        } catch (Throwable t) {
            missingList.clear();
            for (int i = 0; i < partitionCount; i++) {
                if (!plist.contains(i)) {
                    missingList.add(i);
                }
            }
        }

        try {
            List<Future> futures = new ArrayList<Future>(missingList.size());
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
            for (Future future : futures) {
                QueryResult queryResult = (QueryResult) future.get();
                result.addAll(queryResult.getResult());
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return result;
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
