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

package com.hazelcast.collection.impl.txnqueue;

import com.hazelcast.collection.impl.queue.QueueItem;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.queue.operations.SizeOperation;
import com.hazelcast.collection.impl.txnqueue.operations.BaseTxnQueueOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnOfferOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnPeekOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnPollOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnReserveOfferOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnReservePollOperation;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.transaction.TransactionalQueue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.TransactionalDistributedObject;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Provides support for proxy of the Transactional Queue.
 */
public abstract class TransactionalQueueProxySupport<E>
        extends TransactionalDistributedObject<QueueService>
        implements TransactionalQueue<E> {

    protected final String name;
    protected final int partitionId;
    protected final QueueConfig config;

    /** The list of items offered to the transactional queue */
    private final LinkedList<QueueItem> offeredQueue = new LinkedList<QueueItem>();
    /** The IDs of the items modified by the transaction, either added or removed from the queue */
    private final Set<Long> itemIdSet = new HashSet<Long>();

    TransactionalQueueProxySupport(NodeEngine nodeEngine, QueueService service, String name, Transaction tx) {
        super(nodeEngine, service, tx);
        this.name = name;
        partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
        config = nodeEngine.getConfig().findQueueConfig(name);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public final String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    public int size() {
        checkTransactionState();
        SizeOperation operation = new SizeOperation(name);
        try {
            Future<Integer> future = invoke(operation);
            Integer size = future.get();
            return size + offeredQueue.size();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    void checkTransactionState() {
        if (!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    /**
     * Tries to accomodate one more item in the queue in addition to the
     * already offered items. If it succeeds by getting an item ID, it will
     * add the item to the
     * Makes a reservation for a {@link TransactionalQueue#offer} operation
     * and adds the commit operation to the transaction log.
     *
     * @param data    the serialised item being offered
     * @param timeout the wait timeout in milliseconds for the offer reservation
     * @return {@code true} if the item reservation was made
     * @see TxnReserveOfferOperation
     * @see TxnOfferOperation
     */
    boolean offerInternal(Data data, long timeout) {
        TxnReserveOfferOperation operation
                = new TxnReserveOfferOperation(name, timeout, offeredQueue.size(), tx.getTxnId());
        operation.setCallerUuid(tx.getOwnerUuid());
        try {
            Future<Long> future = invoke(operation);
            Long itemId = future.get();
            if (itemId != null) {
                if (!itemIdSet.add(itemId)) {
                    throw new TransactionException("Duplicate itemId: " + itemId);
                }
                offeredQueue.offer(new QueueItem(null, itemId, data));
                TxnOfferOperation txnOfferOperation = new TxnOfferOperation(name, itemId, data);
                putToRecord(txnOfferOperation);
                return true;
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return false;
    }

    Data pollInternal(long timeout) {
        QueueItem reservedOffer = offeredQueue.peek();
        long itemId = reservedOffer == null ? -1 : reservedOffer.getItemId();
        TxnReservePollOperation operation = new TxnReservePollOperation(name, timeout, itemId, tx.getTxnId());
        operation.setCallerUuid(tx.getOwnerUuid());
        try {
            Future<QueueItem> future = invoke(operation);
            QueueItem item = future.get();
            if (item != null) {
                if (reservedOffer != null && item.getItemId() == reservedOffer.getItemId()) {
                    offeredQueue.poll();
                    removeFromRecord(reservedOffer.getItemId());
                    itemIdSet.remove(reservedOffer.getItemId());
                    return reservedOffer.getSerializedObject();
                }
                //
                if (!itemIdSet.add(item.getItemId())) {
                    throw new TransactionException("Duplicate itemId: " + item.getItemId());
                }
                TxnPollOperation txnPollOperation = new TxnPollOperation(name, item.getItemId());
                putToRecord(txnPollOperation);
                return item.getSerializedObject();
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return null;
    }

    Data peekInternal(long timeout) {
        QueueItem offer = offeredQueue.peek();
        long itemId = offer == null ? -1 : offer.getItemId();
        TxnPeekOperation operation = new TxnPeekOperation(name, timeout, itemId, tx.getTxnId());
        try {
            Future<QueueItem> future = invoke(operation);
            QueueItem item = future.get();
            if (item != null) {
                if (offer != null && item.getItemId() == offer.getItemId()) {
                    return offer.getSerializedObject();
                }
                return item.getSerializedObject();
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return null;
    }

    private void putToRecord(BaseTxnQueueOperation operation) {
        QueueTransactionLogRecord logRecord = (QueueTransactionLogRecord) tx.get(name);
        if (logRecord == null) {
            logRecord = new QueueTransactionLogRecord(tx.getTxnId(), name, partitionId);
            tx.add(logRecord);
        }
        logRecord.addOperation(operation);
    }

    private void removeFromRecord(long itemId) {
        QueueTransactionLogRecord logRecord = (QueueTransactionLogRecord) tx.get(name);
        int size = logRecord.removeOperation(itemId);
        if (size == 0) {
            tx.remove(name);
        }
    }

    private <T> InternalCompletableFuture<T> invoke(Operation operation) {
        OperationService operationService = getNodeEngine().getOperationService();
        return operationService.invokeOnPartition(QueueService.SERVICE_NAME, operation, partitionId);
    }
}
