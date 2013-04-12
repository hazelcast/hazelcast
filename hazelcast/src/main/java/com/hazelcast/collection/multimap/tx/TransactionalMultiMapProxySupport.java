/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.util.ExceptionUtil;

import java.util.*;
import java.util.concurrent.Future;

/**
 * @ali 3/29/13
 */
public abstract class TransactionalMultiMapProxySupport extends AbstractDistributedObject<CollectionService> implements TransactionalObject {

    protected final CollectionProxyId proxyId;
    protected final Transaction tx;
    protected final MultiMapConfig config;

    private final Map<Data, Collection<CollectionRecord>> txMap = new HashMap<Data, Collection<CollectionRecord>>();

    protected TransactionalMultiMapProxySupport(NodeEngine nodeEngine, CollectionService service, CollectionProxyId proxyId, Transaction tx) {
        super(nodeEngine, service);
        this.proxyId = proxyId;
        this.tx = tx;
        this.config = nodeEngine.getConfig().getMultiMapConfig(proxyId.getName());
    }

    public boolean putInternal(Data key, Data value) {
        throwExceptionIfNull(key);
        throwExceptionIfNull(value);
        Collection<CollectionRecord> coll = txMap.get(key);
        long timeout = tx.getTimeoutMillis();
        long ttl = timeout*3/2;
        CollectionResponse response = lockAndGet(key, timeout, ttl, coll == null);
        if (response == null){
            throw new ConcurrentModificationException("Transaction couldn't obtain lock " + getThreadId());
        }
        long recordId = (Long)response.getAttachment();
        if (coll == null){
            coll = response.getRecordCollection(getNodeEngine());
            txMap.put(key, coll);
        }
        TxnPutOperation operation = new TxnPutOperation(proxyId, key, value, recordId, getThreadId());
        tx.addTransactionLog(new MultiMapTransactionLog(key, proxyId, ttl, getThreadId(), operation));
        CollectionRecord record = new CollectionRecord(recordId, config.isBinary() ? value : getNodeEngine().toObject(value));
        return coll.add(record);
    }

    public boolean removeInternal(Data key, Data value) {
        throwExceptionIfNull(key);
        throwExceptionIfNull(value);
        Collection<CollectionRecord> coll = txMap.get(key);
        long timeout = tx.getTimeoutMillis();
        long ttl = timeout*3/2;
        CollectionResponse response = lockAndGet(key, timeout, ttl, coll == null);
        if (response == null){
            throw new ConcurrentModificationException("Transaction couldn't obtain lock " + getThreadId());
        }
        if (coll == null){
            coll = response.getRecordCollection(getNodeEngine());
            txMap.put(key, coll);
        }
        CollectionRecord record = new CollectionRecord(-1, config.isBinary() ? value : getNodeEngine().toObject(value));
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
        TxnRemoveOperation operation = new TxnRemoveOperation(proxyId, key, getThreadId(), recordId, value);
        tx.addTransactionLog(new MultiMapTransactionLog(key, proxyId, ttl, getThreadId(), operation));
        return recordId != -1;
    }

    public CollectionResponse removeAllInternal(Data key){
        throwExceptionIfNull(key);
        Collection<CollectionRecord> coll = txMap.get(key);
        long timeout = tx.getTimeoutMillis();
        long ttl = timeout*3/2;
        CollectionResponse response = lockAndGet(key, timeout, ttl, coll == null);
        if (response == null){
            throw new ConcurrentModificationException("Transaction couldn't obtain lock " + getThreadId());
        }
        if (coll == null){
            coll = response.getRecordCollection(getNodeEngine());
            txMap.put(key, coll);
        }
        TxnRemoveAllOperation operation = new TxnRemoveAllOperation(proxyId, key, getThreadId(), coll);
        tx.addTransactionLog(new MultiMapTransactionLog(key, proxyId, ttl, getThreadId(), operation));
        return response;
    }

    public Collection<CollectionRecord> getInternal(Data key){
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

    public int valueCountInternal(Data key){
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
        return ThreadContext.getThreadId();
    }

    private CollectionResponse lockAndGet(Data key, long timeout, long ttl, boolean getCollection) {
        final NodeEngine nodeEngine = getNodeEngine();
        TxnLockAndGetOperation operation = new TxnLockAndGetOperation(proxyId, key, timeout, ttl, getThreadId());
        operation.setGetCollection(getCollection);
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
}
