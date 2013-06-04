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

import com.hazelcast.config.QueueConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.QueueItem;
import com.hazelcast.queue.QueueService;
import com.hazelcast.queue.SizeOperation;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.util.ExceptionUtil;

import java.util.LinkedList;
import java.util.concurrent.Future;

/**
 * @ali 3/25/13
 */
public abstract class TransactionalQueueProxySupport extends AbstractDistributedObject<QueueService> implements TransactionalObject {

    protected final String name;
    protected final Transaction tx;
    protected final int partitionId;
    private final LinkedList<QueueItem> offerIdQueue = new LinkedList<QueueItem>();
    private final LinkedList<Long> pollIdQueue = new LinkedList<Long>();
    protected final QueueConfig config;

    protected TransactionalQueueProxySupport(NodeEngine nodeEngine, QueueService service, String name, Transaction tx) {
        super(nodeEngine, service);
        this.name = name;
        this.tx = tx;
        partitionId = nodeEngine.getPartitionService().getPartitionId(name);
        config = nodeEngine.getConfig().getQueueConfig(name);
    }

    public boolean offerInternal(Data data, long timeout){
        throwExceptionIfNull(data);
        TxnReserveOfferOperation operation = new TxnReserveOfferOperation(name, timeout, offerIdQueue.size());
        try {
            Invocation invocation = getNodeEngine().getOperationService().createInvocationBuilder(QueueService.SERVICE_NAME, operation, partitionId).build();
            Future<Long> f = invocation.invoke();
            Long itemId = f.get();
            if (itemId != null){
                if(offerIdQueue.contains(itemId) || pollIdQueue.contains(itemId)){
                    throw new TransactionException("Duplicate itemId: " + itemId);
                }
                offerIdQueue.offer(new QueueItem(null, itemId, data));
                tx.addTransactionLog(new QueueTransactionLog(itemId, name, partitionId, new TxnOfferOperation(name, itemId, data)));
                return true;
            }
        } catch (Throwable t) {
            ExceptionUtil.rethrow(t);
        }
        return false;
    }

    public Data pollInternal(long timeout){
        QueueItem reservedOffer = offerIdQueue.peek();
        TxnReservePollOperation operation = new TxnReservePollOperation(name, timeout, reservedOffer == null ? -1 : reservedOffer.getItemId());
        try {
            Invocation invocation = getNodeEngine().getOperationService().createInvocationBuilder(QueueService.SERVICE_NAME, operation, partitionId).build();
            Future<QueueItem> f = invocation.invoke();
            QueueItem item = f.get();
            if (item != null){
                if (reservedOffer != null && item.getItemId() == reservedOffer.getItemId()){
                    offerIdQueue.poll();
                    tx.removeTransactionLog(reservedOffer.getItemId());
                    return reservedOffer.getData();
                }
                if(pollIdQueue.contains(item.getItemId()) || offerIdQueue.contains(item.getItemId())){
                    throw new TransactionException("Duplicate itemId: " + item.getItemId());
                }
                pollIdQueue.offer(item.getItemId());
                tx.addTransactionLog(new QueueTransactionLog(item.getItemId(), name, partitionId, new TxnPollOperation(name, item.getItemId())));
                return item.getData();
            }
        } catch (Throwable t) {
            ExceptionUtil.rethrow(t);
        }
        return null;
    }

    public int size() {
        SizeOperation operation = new SizeOperation(name);
        try {
            Invocation invocation = getNodeEngine().getOperationService().createInvocationBuilder(QueueService.SERVICE_NAME, operation, partitionId).build();
            Future<Integer> f = invocation.invoke();
            Integer size = f.get();
            return size + offerIdQueue.size();
        } catch (Throwable t) {
            ExceptionUtil.rethrow(t);
        }
        return 0;
    }

    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }

    public final String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    private void throwExceptionIfNull(Object o){
        if (o == null){
            throw new NullPointerException("Object is null");
        }
    }
}
