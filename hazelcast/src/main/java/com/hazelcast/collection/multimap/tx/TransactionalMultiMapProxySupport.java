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
import com.hazelcast.collection.CollectionService;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.util.ExceptionUtil;

import java.util.ConcurrentModificationException;
import java.util.concurrent.Future;

/**
 * @ali 3/29/13
 */
public abstract class TransactionalMultiMapProxySupport extends AbstractDistributedObject<CollectionService> implements TransactionalObject {

    protected final CollectionProxyId proxyId;
    protected final Transaction tx;

    protected TransactionalMultiMapProxySupport(NodeEngine nodeEngine, CollectionService service, CollectionProxyId proxyId, Transaction tx) {
        super(nodeEngine, service);
        this.proxyId = proxyId;
        this.tx = tx;
    }

    public boolean putInternal(Data key, Data value) {
        throwExceptionIfNull(key);
        throwExceptionIfNull(value);
        long timeout = tx.getTimeoutMillis();
        long ttl = timeout*3/2;
        Long recordId = (Long)lockAndGet(key, value, timeout, ttl, TxnMultiMapOperation.PUT_OPERATION);
        if (recordId == null){
            throw new ConcurrentModificationException("Transaction couldn't obtain lock " + getThreadId());
        }
        TxnPutOperation operation = new TxnPutOperation(proxyId, key, value, recordId, getThreadId());
        tx.addTransactionLog(new MultiMapTransactionLog(key, recordId, proxyId, ttl, getThreadId(), operation));
        return recordId != -1;
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

    private Object lockAndGet(Data key, Data value, long timeout, long ttl, int operationType) {
        final NodeEngine nodeEngine = getNodeEngine();
        TxnLockAndGetOperation operation = new TxnLockAndGetOperation(proxyId, key, value, timeout, ttl, getThreadId());
        operation.setOperationType(operationType);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(CollectionService.SERVICE_NAME, operation, partitionId).build();
            Future f = invocation.invoke();
            return f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }
}
