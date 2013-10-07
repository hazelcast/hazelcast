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
import com.hazelcast.map.QueryResult;
import com.hazelcast.map.operation.*;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
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

import java.util.*;
import java.util.concurrent.Future;

import static com.hazelcast.map.MapService.SERVICE_NAME;

/**
 * @author mdogan 2/26/13
 */
public abstract class TransactionalMapProxySupport extends AbstractDistributedObject<MapService> implements TransactionalObject {

    protected final String name;
    protected final TransactionSupport tx;
    protected final PartitioningStrategy partitionStrategy;

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
                Invocation invocation = operationService
                        .createInvocationBuilder(SERVICE_NAME, new QueryOperation(name, predicate), member.getAddress()).build();
                Future future = invocation.invoke();
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
                    Future f = operationService.createInvocationBuilder(SERVICE_NAME, queryPartitionOperation, pid).build().invoke();
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

    public String getName() {
        return name;
    }

    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }
}
