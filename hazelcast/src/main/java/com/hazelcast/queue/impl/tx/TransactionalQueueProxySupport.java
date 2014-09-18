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

package com.hazelcast.queue.impl.tx;

import com.hazelcast.config.QueueConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.impl.QueueItem;
import com.hazelcast.queue.impl.QueueService;
import com.hazelcast.queue.impl.SizeOperation;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.ExceptionUtil;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Provides support for proxy of the Transactional Queue.
 */
public abstract class TransactionalQueueProxySupport extends AbstractDistributedObject<QueueService>
        implements TransactionalObject {

    protected final String name;
    protected final TransactionSupport tx;
    protected final int partitionId;
    protected final QueueConfig config;
    private final LinkedList<QueueItem> offeredQueue = new LinkedList<QueueItem>();
    private final Set<Long> itemIdSet = new HashSet<Long>();

    protected TransactionalQueueProxySupport(NodeEngine nodeEngine, QueueService service, String name,
                                             TransactionSupport tx) {
        super(nodeEngine, service);
        this.name = name;
        this.tx = tx;
        partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
        config = nodeEngine.getConfig().findQueueConfig(name);
    }

    protected void checkTransactionState() {
        if (!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    public boolean offerInternal(Data data, long timeout) {
        TxnReserveOfferOperation operation
                = new TxnReserveOfferOperation(name, timeout, offeredQueue.size(), tx.getTxnId());
        try {
            Future<Long> f = invoke(operation);
            Long itemId = f.get();
            if (itemId != null) {
                if (!itemIdSet.add(itemId)) {
                    throw new TransactionException("Duplicate itemId: " + itemId);
                }
                offeredQueue.offer(new QueueItem(null, itemId, data));
                TxnOfferOperation txnOfferOperation = new TxnOfferOperation(name, itemId, data);
                QueueTransactionLog transactionLog = new QueueTransactionLog(
                        tx.getTxnId(), itemId, name, partitionId, txnOfferOperation);
                tx.addTransactionLog(transactionLog);
                return true;
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return false;
    }

    public Data pollInternal(long timeout) {
        QueueItem reservedOffer = offeredQueue.peek();
        long itemId = reservedOffer == null ? -1 : reservedOffer.getItemId();
        TxnReservePollOperation operation = new TxnReservePollOperation(name, timeout, itemId, tx.getTxnId());
        try {
            Future<QueueItem> f = invoke(operation);
            QueueItem item = f.get();
            if (item != null) {
                if (reservedOffer != null && item.getItemId() == reservedOffer.getItemId()) {
                    offeredQueue.poll();
                    tx.removeTransactionLog(new TransactionLogKey(reservedOffer.getItemId(), name));
                    itemIdSet.remove(reservedOffer.getItemId());
                    return reservedOffer.getData();
                }
                //
                if (!itemIdSet.add(item.getItemId())) {
                    throw new TransactionException("Duplicate itemId: " + item.getItemId());
                }
                TxnPollOperation op = new TxnPollOperation(name, item.getItemId());
                QueueTransactionLog transactionLog
                        = new QueueTransactionLog(tx.getTxnId(), item.getItemId(), name, partitionId, op);
                tx.addTransactionLog(transactionLog);
                return item.getData();
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return null;
    }

    public Data peekInternal(long timeout) {
        final QueueItem offer = offeredQueue.peek();
        long itemId = offer == null ? -1 : offer.getItemId();
        final TxnPeekOperation operation = new TxnPeekOperation(name, timeout, itemId, tx.getTxnId());
        try {
            final Future<QueueItem> f = invoke(operation);
            final QueueItem item = f.get();
            if (item != null) {
                if (offer != null && item.getItemId() == offer.getItemId()) {
                    return offer.getData();
                }
                return item.getData();
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return null;
    }

    public int size() {
        checkTransactionState();
        SizeOperation operation = new SizeOperation(name);
        try {
            Future<Integer> f = invoke(operation);
            Integer size = f.get();
            return size + offeredQueue.size();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private <E> InternalCompletableFuture<E> invoke(Operation operation) {
        OperationService operationService = getNodeEngine().getOperationService();
        return operationService.invokeOnPartition(QueueService.SERVICE_NAME, operation, partitionId);
    }

    public String getName() {
        return name;
    }

    public final String getServiceName() {
        return QueueService.SERVICE_NAME;
    }
}
