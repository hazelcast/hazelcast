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

package com.hazelcast.multimap.impl.txn;

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.operations.CountOperation;
import com.hazelcast.multimap.impl.operations.GetAllOperation;
import com.hazelcast.multimap.impl.operations.MultiMapOperationFactory;
import com.hazelcast.multimap.impl.operations.MultiMapResponse;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Future;

public abstract class TransactionalMultiMapProxySupport extends AbstractDistributedObject<MultiMapService>
        implements TransactionalObject {

    private static final double TIMEOUT_EXTEND_MULTIPLIER = 1.5;
    protected final String name;
    protected final TransactionSupport tx;
    protected final MultiMapConfig config;
    private long version = -1;

    private final Map<Data, Collection<MultiMapRecord>> txMap = new HashMap<Data, Collection<MultiMapRecord>>();

    protected TransactionalMultiMapProxySupport(NodeEngine nodeEngine,
                                                MultiMapService service,
                                                String name,
                                                TransactionSupport tx) {
        super(nodeEngine, service);
        this.name = name;
        this.tx = tx;
        this.config = nodeEngine.getConfig().findMultiMapConfig(name);
    }

    protected void checkTransactionState() {
        if (!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    protected boolean putInternal(Data key, Data value) {
        throwExceptionIfNull(key);
        throwExceptionIfNull(value);
        Collection<MultiMapRecord> coll = txMap.get(key);
        long recordId = -1;
        long timeout = tx.getTimeoutMillis();
        long ttl = extendTimeout(timeout);
        final MultiMapTransactionLog log;
        if (coll == null) {
            MultiMapResponse response = lockAndGet(key, timeout, ttl);
            if (response == null) {
                throw new ConcurrentModificationException("Transaction couldn't obtain lock " + getThreadId());
            }
            recordId = response.getNextRecordId();
            version = response.getTxVersion();
            coll = createCollection(response.getRecordCollection(getNodeEngine()));
            txMap.put(key, coll);
            log = new MultiMapTransactionLog(key, name, ttl, getThreadId(), version);
            tx.addTransactionLog(log);
        } else {
            log = (MultiMapTransactionLog) tx.getTransactionLog(getTxLogKey(key));
        }
        MultiMapRecord record = new MultiMapRecord(config.isBinary() ? value : getNodeEngine().toObject(value));
        if (coll.add(record)) {
            if (recordId == -1) {
                recordId = nextId(key);
            }
            record.setRecordId(recordId);
            TxnPutOperation operation = new TxnPutOperation(name, key, value, recordId);
            log.addOperation(operation);
            return true;
        }
        return false;
    }

    protected boolean removeInternal(Data key, Data value) {
        throwExceptionIfNull(key);
        throwExceptionIfNull(value);
        Collection<MultiMapRecord> coll = txMap.get(key);
        long timeout = tx.getTimeoutMillis();
        long ttl = extendTimeout(timeout);
        final MultiMapTransactionLog log;
        if (coll == null) {
            MultiMapResponse response = lockAndGet(key, timeout, ttl);
            if (response == null) {
                throw new ConcurrentModificationException("Transaction couldn't obtain lock " + getThreadId());
            }
            version = response.getTxVersion();
            coll = createCollection(response.getRecordCollection(getNodeEngine()));
            txMap.put(key, coll);
            log = new MultiMapTransactionLog(key, name, ttl, getThreadId(), version);
            tx.addTransactionLog(log);
        } else {
            log = (MultiMapTransactionLog) tx.getTransactionLog(getTxLogKey(key));
        }
        MultiMapRecord record = new MultiMapRecord(config.isBinary() ? value : getNodeEngine().toObject(value));
        Iterator<MultiMapRecord> iterator = coll.iterator();
        long recordId = -1;
        while (iterator.hasNext()) {
            MultiMapRecord r = iterator.next();
            if (r.equals(record)) {
                iterator.remove();
                recordId = r.getRecordId();
                break;
            }
        }
        if (version != -1 || recordId != -1) {
            TxnRemoveOperation operation = new TxnRemoveOperation(name, key, recordId, value);
            log.addOperation(operation);
            return recordId != -1;
        }
        return false;
    }

    protected Collection<MultiMapRecord> removeAllInternal(Data key) {
        throwExceptionIfNull(key);
        long timeout = tx.getTimeoutMillis();
        long ttl = extendTimeout(timeout);
        Collection<MultiMapRecord> coll = txMap.get(key);
        final MultiMapTransactionLog log;
        if (coll == null) {
            MultiMapResponse response = lockAndGet(key, timeout, ttl);
            if (response == null) {
                throw new ConcurrentModificationException("Transaction couldn't obtain lock " + getThreadId());
            }
            version = response.getTxVersion();
            coll = createCollection(response.getRecordCollection(getNodeEngine()));
            log = new MultiMapTransactionLog(key, name, ttl, getThreadId(), version);
            tx.addTransactionLog(log);
        } else {
            log = (MultiMapTransactionLog) tx.getTransactionLog(getTxLogKey(key));
        }
        txMap.put(key, createCollection());
        TxnRemoveAllOperation operation = new TxnRemoveAllOperation(name, key, coll);
        log.addOperation(operation);
        return coll;
    }

    protected Collection<MultiMapRecord> getInternal(Data key) {
        throwExceptionIfNull(key);
        Collection<MultiMapRecord> coll = txMap.get(key);
        if (coll == null) {
            GetAllOperation operation = new GetAllOperation(name, key);
            try {
                int partitionId = getNodeEngine().getPartitionService().getPartitionId(key);
                final OperationService operationService = getNodeEngine().getOperationService();
                Future<MultiMapResponse> f = operationService.invokeOnPartition(
                        MultiMapService.SERVICE_NAME,
                        operation,
                        partitionId
                );
                MultiMapResponse response = f.get();
                coll = response.getRecordCollection(getNodeEngine());
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }

        }
        return coll;
    }

    protected int valueCountInternal(Data key) {
        throwExceptionIfNull(key);
        Collection<MultiMapRecord> coll = txMap.get(key);
        if (coll == null) {
            CountOperation operation = new CountOperation(name, key);
            try {
                int partitionId = getNodeEngine().getPartitionService().getPartitionId(key);
                final OperationService operationService = getNodeEngine().getOperationService();
                Future<Integer> f = operationService
                        .invokeOnPartition(MultiMapService.SERVICE_NAME, operation, partitionId);
                return f.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
        return coll.size();
    }

    public int size() {
        checkTransactionState();
        try {
            final OperationService operationService = getNodeEngine().getOperationService();
            final Map<Integer, Object> results = operationService.invokeOnAllPartitions(MultiMapService.SERVICE_NAME,
                    new MultiMapOperationFactory(name, MultiMapOperationFactory.OperationFactoryType.SIZE));
            int size = 0;
            for (Object obj : results.values()) {
                if (obj == null) {
                    continue;
                }
                Integer result = getNodeEngine().toObject(obj);
                size += result;
            }
            for (Data key : txMap.keySet()) {
                MultiMapTransactionLog log = (MultiMapTransactionLog) tx.getTransactionLog(getTxLogKey(key));
                if (log != null) {
                    size += log.size();
                }
            }

            return size;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private TransactionLogKey getTxLogKey(Data key) {
        return new TransactionLogKey(name, key);
    }

    public final String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    private void throwExceptionIfNull(Object o) {
        if (o == null) {
            throw new NullPointerException("Object is null");
        }
    }

    private long getThreadId() {
        return ThreadUtil.getThreadId();
    }

    private MultiMapResponse lockAndGet(Data key, long timeout, long ttl) {
        final NodeEngine nodeEngine = getNodeEngine();
        TxnLockAndGetOperation operation = new TxnLockAndGetOperation(name, key, timeout, ttl, getThreadId());
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            final OperationService operationService = nodeEngine.getOperationService();
            Future<MultiMapResponse> f = operationService.
                    invokeOnPartition(MultiMapService.SERVICE_NAME, operation, partitionId);
            return f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private long nextId(Data key) {
        final NodeEngine nodeEngine = getNodeEngine();
        TxnGenerateRecordIdOperation operation = new TxnGenerateRecordIdOperation(name, key);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            final OperationService operationService = nodeEngine.getOperationService();
            Future<Long> f = operationService.invokeOnPartition(MultiMapService.SERVICE_NAME, operation, partitionId);
            return f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private Collection<MultiMapRecord> createCollection() {
        if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.SET)) {
            return new HashSet<MultiMapRecord>();
        } else if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.LIST)) {
            return new ArrayList<MultiMapRecord>();
        }
        return null;
    }

    private Collection<MultiMapRecord> createCollection(Collection<MultiMapRecord> coll) {
        if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.SET)) {
            return new HashSet<MultiMapRecord>(coll);
        } else if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.LIST)) {
            return new ArrayList<MultiMapRecord>(coll);
        }
        return null;
    }

    private long extendTimeout(long timeout) {
        return (long) (timeout * TIMEOUT_EXTEND_MULTIPLIER);
    }
}
