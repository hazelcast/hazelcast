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

package com.hazelcast.collection.multimap.tx;

import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.operations.CollectionResponse;
import com.hazelcast.collection.operations.CountOperation;
import com.hazelcast.collection.operations.GetAllOperation;
import com.hazelcast.collection.operations.MultiMapOperationFactory;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;

import java.util.*;
import java.util.concurrent.Future;

/**
 * @author ali 3/29/13
 */
public abstract class TransactionalMultiMapProxySupport extends AbstractDistributedObject<CollectionService> implements TransactionalObject {

    protected final CollectionProxyId proxyId;
    protected final TransactionSupport tx;
    protected final MultiMapConfig config;
    private long version = -1;

    private final Map<Data, Collection<CollectionRecord>> txMap = new HashMap<Data, Collection<CollectionRecord>>();

    protected TransactionalMultiMapProxySupport(NodeEngine nodeEngine, CollectionService service, CollectionProxyId proxyId, TransactionSupport tx, MultiMapConfig config) {
        super(nodeEngine, service);
        this.proxyId = proxyId;
        this.tx = tx;
        this.config = config;
    }

    protected void checkTransactionState(){
        if(!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    protected boolean putInternal(Data key, Data value) {
        throwExceptionIfNull(key);
        throwExceptionIfNull(value);
        Collection<CollectionRecord> coll = txMap.get(key);
        long recordId = -1;
        long timeout = tx.getTimeoutMillis();
        long ttl = timeout*3/2;
        final MultiMapTransactionLog log;
        if (coll == null){
            CollectionResponse response = lockAndGet(key, timeout, ttl);
            if (response == null){
                throw new ConcurrentModificationException("Transaction couldn't obtain lock " + getThreadId());
            }
            recordId = response.getNextRecordId();
            version = response.getTxVersion();
            coll =  createCollection(response.getRecordCollection(getNodeEngine()));
            txMap.put(key, coll);
            log = new MultiMapTransactionLog(key, proxyId, ttl, getThreadId(), version);
            tx.addTransactionLog(log);
        } else {
            log = (MultiMapTransactionLog)tx.getTransactionLog(getTxLogKey(key));
        }
        CollectionRecord record = new CollectionRecord(config.isBinary() ? value : getNodeEngine().toObject(value));
        if (coll.add(record)){
            if (recordId == -1){
                recordId = nextId(key);
            }
            record.setRecordId(recordId);
            TxnPutOperation operation = new TxnPutOperation(proxyId, key, value, recordId);
            log.addOperation(operation);
            return true;
        }
        return false;
    }

    protected boolean removeInternal(Data key, Data value) {
        throwExceptionIfNull(key);
        throwExceptionIfNull(value);
        Collection<CollectionRecord> coll = txMap.get(key);
        long timeout = tx.getTimeoutMillis();
        long ttl = timeout*3/2;
        final MultiMapTransactionLog log;
        if (coll == null){
            CollectionResponse response = lockAndGet(key, timeout, ttl);
            if (response == null){
                throw new ConcurrentModificationException("Transaction couldn't obtain lock " + getThreadId());
            }
            version = response.getTxVersion();
            coll =  createCollection(response.getRecordCollection(getNodeEngine()));
            txMap.put(key, coll);
            log = new MultiMapTransactionLog(key, proxyId, ttl, getThreadId(), version);
            tx.addTransactionLog(log);
        } else {
            log = (MultiMapTransactionLog)tx.getTransactionLog(getTxLogKey(key));
        }
        CollectionRecord record = new CollectionRecord(config.isBinary() ? value : getNodeEngine().toObject(value));
        Iterator<CollectionRecord> iterator = coll.iterator();
        long recordId = -1;
        while (iterator.hasNext()){
            CollectionRecord r = iterator.next();
            if (r.equals(record)){
                iterator.remove();
                recordId = r.getRecordId();
                break;
            }
        }
        if (version != -1 || recordId != -1){
            TxnRemoveOperation operation = new TxnRemoveOperation(proxyId, key, recordId, value);
            log.addOperation(operation);
            return recordId != -1;
        }
        return false;
    }

    protected Collection<CollectionRecord> removeAllInternal(Data key){
        throwExceptionIfNull(key);
        long timeout = tx.getTimeoutMillis();
        long ttl = timeout*3/2;
        Collection<CollectionRecord> coll = txMap.get(key);
        final MultiMapTransactionLog log;
        if (coll == null){
            CollectionResponse response = lockAndGet(key, timeout, ttl);
            if (response == null){
                throw new ConcurrentModificationException("Transaction couldn't obtain lock " + getThreadId());
            }
            version = response.getTxVersion();
            coll =  createCollection(response.getRecordCollection(getNodeEngine()));
            log = new MultiMapTransactionLog(key, proxyId, ttl, getThreadId(), version);
            tx.addTransactionLog(log);
        } else {
            log = (MultiMapTransactionLog)tx.getTransactionLog(getTxLogKey(key));
        }
        txMap.put(key, createCollection());
        TxnRemoveAllOperation operation = new TxnRemoveAllOperation(proxyId, key, coll);
        log.addOperation(operation);
        return coll;
    }

    protected Collection<CollectionRecord> getInternal(Data key){
        throwExceptionIfNull(key);
        Collection<CollectionRecord> coll = txMap.get(key);
        if (coll == null){
            GetAllOperation operation = new GetAllOperation(proxyId, key);
            try {
                int partitionId = getNodeEngine().getPartitionService().getPartitionId(key);
                Invocation invocation = getNodeEngine().getOperationService()
                        .createInvocationBuilder(CollectionService.SERVICE_NAME, operation, partitionId).build();
                Future<CollectionResponse> f = invocation.invoke();
                CollectionResponse response = f.get();
                coll = response.getRecordCollection(getNodeEngine());
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }

        }
        return coll;
    }

    protected int valueCountInternal(Data key){
        throwExceptionIfNull(key);
        Collection<CollectionRecord> coll = txMap.get(key);
        if (coll == null){
            CountOperation operation = new CountOperation(proxyId, key);
            try {
                int partitionId = getNodeEngine().getPartitionService().getPartitionId(key);
                Invocation invocation = getNodeEngine().getOperationService()
                        .createInvocationBuilder(CollectionService.SERVICE_NAME, operation, partitionId).build();
                Future<Integer> f = invocation.invoke();
                return f.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
        return coll.size();
    }

    public int size(){
        checkTransactionState();
        try {
            final Map<Integer, Object> results = getNodeEngine().getOperationService().invokeOnAllPartitions(CollectionService.SERVICE_NAME,
                    new MultiMapOperationFactory(proxyId, MultiMapOperationFactory.OperationFactoryType.SIZE));
            int size = 0;
            for (Object obj : results.values()) {
                if (obj == null) {
                    continue;
                }
                Integer result = getNodeEngine().toObject(obj);
                size += result;
            }
            for (Data key : txMap.keySet()) {
                MultiMapTransactionLog log = (MultiMapTransactionLog)tx.getTransactionLog(getTxLogKey(key));
                if (log != null){
                    size += log.size();
                }
            }

            return size;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private TransactionLogKey getTxLogKey(Data key) {
        return new TransactionLogKey(proxyId, key);
    }

    public Object getId() {
        return proxyId;
    }

    public String getName() {
        return proxyId.getName();
    }

    public final String getServiceName() {
        return CollectionService.SERVICE_NAME;
    }

    private void throwExceptionIfNull(Object o) {
        if (o == null) {
            throw new NullPointerException("Object is null");
        }
    }

    private int getThreadId() {
        return ThreadUtil.getThreadId();
    }

    private CollectionResponse lockAndGet(Data key, long timeout, long ttl) {
        final NodeEngine nodeEngine = getNodeEngine();
        TxnLockAndGetOperation operation = new TxnLockAndGetOperation(proxyId, key, timeout, ttl, getThreadId());
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(CollectionService.SERVICE_NAME, operation, partitionId).build();
            Future<CollectionResponse> f = invocation.invoke();
            return f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private long nextId(Data key){
        final NodeEngine nodeEngine = getNodeEngine();
        TxnGenerateRecordIdOperation operation = new TxnGenerateRecordIdOperation(proxyId, key);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(CollectionService.SERVICE_NAME, operation, partitionId).build();
            Future<Long> f = invocation.invoke();
            return f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private Collection<CollectionRecord> createCollection(){
        if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.SET)){
            return new HashSet<CollectionRecord>();
        }
        else if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.LIST)){
            return new ArrayList<CollectionRecord>();
        }
        return null;
    }

    private Collection<CollectionRecord> createCollection(Collection<CollectionRecord> coll){
        if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.SET)){
            return new HashSet<CollectionRecord>(coll);
        }
        else if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.LIST)){
            return new ArrayList<CollectionRecord>(coll);
        }
        return null;
    }
}
