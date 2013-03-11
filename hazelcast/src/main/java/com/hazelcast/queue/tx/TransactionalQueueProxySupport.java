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

package com.hazelcast.queue.tx;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @mdogan 3/8/13
 */
public abstract class TransactionalQueueProxySupport<E> extends AbstractDistributedObject<QueueService> implements TransactionalObject {

    protected final String name;
    protected final Transaction tx;
    protected final int partitionId;

    public TransactionalQueueProxySupport(String name, NodeEngine nodeEngine, QueueService service, Transaction tx) {
        super(nodeEngine, service);
        this.name = name;
        this.tx = tx;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(nodeEngine.toData(name));
    }

    public boolean offerInternal(Data data, long timeout, TimeUnit unit) throws InterruptedException, TransactionException {
        if (tx.getState() != Transaction.State.ACTIVE) {
            throw new IllegalStateException("Transaction is not active!");
        }
        tx.addPartition(partitionId);
        TxOfferOperation operation = new TxOfferOperation(name, tx.getTxnId(), getTimeout(timeout, unit) , data);
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(getServiceName(), operation, partitionId).build();
            Future f = inv.invoke();
            return (Boolean) f.get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrowAllowInterrupted(throwable);
        }
    }

    private long getTimeout(long timeout, TimeUnit unit) {
        final long timeoutMillis = unit.toMillis(timeout);
        return timeoutMillis > tx.getTimeoutMillis() ? tx.getTimeoutMillis() : timeoutMillis;
    }

    public Data pollInternal(long timeout, TimeUnit unit) throws InterruptedException, TransactionException {
        if (tx.getState() != Transaction.State.ACTIVE) {
            throw new IllegalStateException("Transaction is not active!");
        }
        tx.addPartition(partitionId);
        return null;
    }

    public Data peekInternal() throws TransactionException {
        return null;
    }

    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }

    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }
}
