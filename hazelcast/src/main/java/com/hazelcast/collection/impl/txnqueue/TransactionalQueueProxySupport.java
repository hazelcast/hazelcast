/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.TransactionalDistributedObject;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * Provides support for proxy of the Transactional Queue.
 */
public abstract class TransactionalQueueProxySupport extends TransactionalDistributedObject<QueueService>
        implements TransactionalObject {

    protected final String name;
    protected final int partitionId;
    protected final QueueConfig config;
    private final LinkedList<QueueItem> offeredQueue = new LinkedList<QueueItem>();
    private final Set<Long> itemIdSet = new HashSet<Long>();

    protected TransactionalQueueProxySupport(NodeEngine nodeEngine, QueueService service, String name,
                                             Transaction tx) {
        super(nodeEngine, service, tx);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
        this.config = nodeEngine.getConfig().findQueueConfig(name);
    }

    protected void checkTransactionState() {
        if (!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    public boolean offerInternal(Data data, long timeout) {
        Operation operation = new TxnReserveOfferOperation(name, timeout, offeredQueue.size(), tx.getTxnId())
                .setPartitionId(partitionId);
        InternalCompletableFuture<Long> f = invokeOnPartition(operation);
        Long itemId = f.join();
        if (itemId != null) {
            if (!itemIdSet.add(itemId)) {
                throw new TransactionException("Duplicate itemId: " + itemId);
            }
            offeredQueue.offer(new QueueItem(null, itemId, data));
            TxnOfferOperation txnOfferOperation = new TxnOfferOperation(name, itemId, data);
            putToRecord(txnOfferOperation);
            return true;
        }
        return false;
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

    public Data pollInternal(long timeout) {
        QueueItem reservedOffer = offeredQueue.peek();
        long itemId = reservedOffer == null ? -1 : reservedOffer.getItemId();
        Operation operation = new TxnReservePollOperation(name, timeout, itemId, tx.getTxnId())
                .setPartitionId(partitionId);
        InternalCompletableFuture<QueueItem> f = invokeOnPartition(operation);
        QueueItem item = f.join();
        if (item != null) {
            if (reservedOffer != null && item.getItemId() == reservedOffer.getItemId()) {
                offeredQueue.poll();
                removeFromRecord(reservedOffer.getItemId());
                itemIdSet.remove(reservedOffer.getItemId());
                return reservedOffer.getData();
            }
            //
            if (!itemIdSet.add(item.getItemId())) {
                throw new TransactionException("Duplicate itemId: " + item.getItemId());
            }
            TxnPollOperation txnPollOperation = new TxnPollOperation(name, item.getItemId());
            putToRecord(txnPollOperation);
            return item.getData();
        }
        return null;
    }

    public Data peekInternal(long timeout) {
        QueueItem offer = offeredQueue.peek();
        long itemId = offer == null ? -1 : offer.getItemId();
        Operation operation = new TxnPeekOperation(name, timeout, itemId, tx.getTxnId())
                .setPartitionId(partitionId);
        InternalCompletableFuture<QueueItem> f = invokeOnPartition(operation);
        QueueItem item = f.join();
        if (item != null) {
            if (offer != null && item.getItemId() == offer.getItemId()) {
                return offer.getData();
            }
            return item.getData();
        }
        return null;
    }

    public int size() {
        checkTransactionState();
        Operation operation = new SizeOperation(name).setPartitionId(partitionId);
        InternalCompletableFuture<Integer> f = invokeOnPartition(operation);
        Integer size = f.join();
        return size + offeredQueue.size();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public final String getServiceName() {
        return QueueService.SERVICE_NAME;
    }
}
