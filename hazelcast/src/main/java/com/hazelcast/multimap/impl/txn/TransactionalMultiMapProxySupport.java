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

package com.hazelcast.multimap.impl.txn;

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.transaction.TransactionalMultiMap;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.operations.CountOperation;
import com.hazelcast.multimap.impl.operations.GetAllOperation;
import com.hazelcast.multimap.impl.operations.MultiMapOperationFactory;
import com.hazelcast.multimap.impl.operations.MultiMapResponse;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.TransactionalDistributedObject;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.internal.util.ThreadUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public abstract class TransactionalMultiMapProxySupport<K, V>
        extends TransactionalDistributedObject<MultiMapService>
        implements TransactionalMultiMap<K, V> {

    private static final double TIMEOUT_EXTEND_MULTIPLIER = 1.5;

    protected final String name;
    protected final MultiMapConfig config;

    private final Map<Data, Collection<MultiMapRecord>> txMap = new HashMap<Data, Collection<MultiMapRecord>>();

    private final OperationService operationService;
    private final IPartitionService partitionService;

    TransactionalMultiMapProxySupport(NodeEngine nodeEngine, MultiMapService service, String name, Transaction tx) {
        super(nodeEngine, service, tx);
        this.name = name;
        this.config = nodeEngine.getConfig().findMultiMapConfig(name);
        this.operationService = nodeEngine.getOperationService();
        this.partitionService = nodeEngine.getPartitionService();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public final String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    @Override
    public int size() {
        checkTransactionActive();

        try {
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(MultiMapService.SERVICE_NAME,
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
                MultiMapTransactionLogRecord log = (MultiMapTransactionLogRecord) tx.get(getRecordLogKey(key));
                if (log != null) {
                    size += log.size();
                }
            }
            return size;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    void checkTransactionActive() {
        if (!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    boolean putInternal(Data key, Data value) {
        checkObjectNotNull(key);
        checkObjectNotNull(value);

        Collection<MultiMapRecord> coll = txMap.get(key);
        long recordId = -1;
        long timeout = tx.getTimeoutMillis();
        long ttl = extendTimeout(timeout);
        MultiMapTransactionLogRecord logRecord;
        if (coll == null) {
            MultiMapResponse response = lockAndGet(key, timeout, ttl);
            if (response == null) {
                throw new ConcurrentModificationException("Transaction couldn't obtain lock " + getThreadId());
            }
            recordId = response.getNextRecordId();
            coll = createCollection(response.getRecordCollection(getNodeEngine()));
            txMap.put(key, coll);
            logRecord = new MultiMapTransactionLogRecord(getPartitionId(key), key, name, ttl, getThreadId());
            tx.add(logRecord);
        } else {
            logRecord = (MultiMapTransactionLogRecord) tx.get(getRecordLogKey(key));
        }
        MultiMapRecord record = new MultiMapRecord(config.isBinary() ? value : toObjectIfNeeded(value));
        if (coll.add(record)) {
            if (recordId == -1) {
                recordId = nextId(key);
            }
            record.setRecordId(recordId);
            TxnPutOperation operation = new TxnPutOperation(name, key, value, recordId);
            logRecord.addOperation(operation);
            return true;
        }
        return false;
    }

    boolean removeInternal(Data key, Data value) {
        checkObjectNotNull(key);
        checkObjectNotNull(value);

        Collection<MultiMapRecord> coll = txMap.get(key);
        long timeout = tx.getTimeoutMillis();
        long ttl = extendTimeout(timeout);
        MultiMapTransactionLogRecord logRecord;
        if (coll == null) {
            MultiMapResponse response = lockAndGet(key, timeout, ttl);
            if (response == null) {
                throw new ConcurrentModificationException("Transaction couldn't obtain lock " + getThreadId());
            }
            coll = createCollection(response.getRecordCollection(getNodeEngine()));
            txMap.put(key, coll);
            logRecord = new MultiMapTransactionLogRecord(getPartitionId(key), key, name, ttl, getThreadId());
            tx.add(logRecord);
        } else {
            logRecord = (MultiMapTransactionLogRecord) tx.get(getRecordLogKey(key));
        }
        MultiMapRecord record = new MultiMapRecord(config.isBinary() ? value : toObjectIfNeeded(value));
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
        if (recordId != -1) {
            TxnRemoveOperation operation = new TxnRemoveOperation(name, key, recordId, value);
            logRecord.addOperation(operation);
            return true;
        }
        return false;
    }

    Collection<MultiMapRecord> removeAllInternal(Data key) {
        checkObjectNotNull(key);

        long timeout = tx.getTimeoutMillis();
        long ttl = extendTimeout(timeout);
        Collection<MultiMapRecord> coll = txMap.get(key);
        MultiMapTransactionLogRecord logRecord;
        if (coll == null) {
            MultiMapResponse response = lockAndGet(key, timeout, ttl);
            if (response == null) {
                throw new ConcurrentModificationException("Transaction couldn't obtain lock " + getThreadId());
            }
            coll = createCollection(response.getRecordCollection(getNodeEngine()));
            logRecord = new MultiMapTransactionLogRecord(getPartitionId(key), key, name, ttl, getThreadId());
            tx.add(logRecord);
        } else {
            logRecord = (MultiMapTransactionLogRecord) tx.get(getRecordLogKey(key));
        }
        txMap.put(key, createCollection());
        TxnRemoveAllOperation operation = new TxnRemoveAllOperation(name, key, coll);
        logRecord.addOperation(operation);
        return coll;
    }

    Collection<MultiMapRecord> getInternal(Data key) {
        checkObjectNotNull(key);

        Collection<MultiMapRecord> coll = txMap.get(key);
        if (coll == null) {
            GetAllOperation operation = new GetAllOperation(name, key);
            operation.setThreadId(ThreadUtil.getThreadId());
            try {
                int partitionId = partitionService.getPartitionId(key);
                Future<MultiMapResponse> future = operationService
                        .invokeOnPartition(MultiMapService.SERVICE_NAME, operation, partitionId);
                MultiMapResponse response = future.get();
                coll = response.getRecordCollection(getNodeEngine());
            } catch (Throwable t) {
                throw rethrow(t);
            }
        }
        return coll;
    }

    int valueCountInternal(Data key) {
        checkObjectNotNull(key);

        Collection<MultiMapRecord> coll = txMap.get(key);
        if (coll == null) {
            CountOperation operation = new CountOperation(name, key);
            operation.setThreadId(ThreadUtil.getThreadId());
            try {
                int partitionId = partitionService.getPartitionId(key);
                Future<Integer> future = operationService.invokeOnPartition(MultiMapService.SERVICE_NAME, operation, partitionId);
                return future.get();
            } catch (Throwable t) {
                throw rethrow(t);
            }
        }
        return coll.size();
    }

    private TransactionRecordKey getRecordLogKey(Data key) {
        return new TransactionRecordKey(name, key);
    }

    private void checkObjectNotNull(Object o) {
        checkNotNull(o, "Object is null");
    }

    private long getThreadId() {
        return ThreadUtil.getThreadId();
    }

    private MultiMapResponse lockAndGet(Data key, long timeout, long ttl) {
        boolean blockReads = tx.getTransactionType() == TransactionType.ONE_PHASE;
        TxnLockAndGetOperation operation = new TxnLockAndGetOperation(name, key, timeout, ttl, getThreadId(), blockReads);
        try {
            int partitionId = partitionService.getPartitionId(key);
            Future<MultiMapResponse> future = operationService
                    .invokeOnPartition(MultiMapService.SERVICE_NAME, operation, partitionId);
            return future.get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private long nextId(Data key) {
        TxnGenerateRecordIdOperation operation = new TxnGenerateRecordIdOperation(name, key);
        try {
            int partitionId = partitionService.getPartitionId(key);
            Future<Long> future = operationService.invokeOnPartition(MultiMapService.SERVICE_NAME, operation, partitionId);
            return future.get();
        } catch (Throwable t) {
            throw rethrow(t);
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
