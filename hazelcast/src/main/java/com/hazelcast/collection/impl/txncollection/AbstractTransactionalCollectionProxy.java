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

package com.hazelcast.collection.impl.txncollection;

import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.collection.impl.collection.operations.CollectionSizeOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionReserveAddOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionReserveRemoveOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionTxnAddOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionTxnRemoveOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.TransactionalDistributedObject;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.impl.Transaction;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.hazelcast.util.Preconditions.checkNotNull;

public abstract class AbstractTransactionalCollectionProxy<S extends RemoteService, E> extends TransactionalDistributedObject<S> {

    protected final String name;
    protected final int partitionId;
    protected final Set<Long> itemIdSet = new HashSet<Long>();

    public AbstractTransactionalCollectionProxy(String name, Transaction tx, NodeEngine nodeEngine, S service) {
        super(nodeEngine, service, tx);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    protected abstract Collection<CollectionItem> getCollection();

    @Override
    public String getName() {
        return name;
    }

    public boolean add(E e) {
        checkTransactionActive();
        checkObjectNotNull(e);

        Data value = toData(e);
        Operation operation = new CollectionReserveAddOperation(name, tx.getTxnId(), null)
                .setServiceName(getServiceName())
                .setPartitionId(partitionId);

        InternalCompletableFuture<Long> f = invokeOnPartition(operation);
        Long itemId = f.join();
        if (itemId != null) {
            if (!itemIdSet.add(itemId)) {
                throw new TransactionException("Duplicate itemId: " + itemId);
            }
            getCollection().add(new CollectionItem(itemId, value));
            CollectionTxnAddOperation op = new CollectionTxnAddOperation(name, itemId, value);
            putToRecord(op);
            return true;
        }
        return false;
    }

    protected void putToRecord(CollectionTxnOperation operation) {
        CollectionTransactionLogRecord logRecord = (CollectionTransactionLogRecord) tx.get(name);
        if (logRecord == null) {
            logRecord = new CollectionTransactionLogRecord(getServiceName(), tx.getTxnId(), name, partitionId);
            tx.add(logRecord);
        }
        logRecord.addOperation(operation);
    }

    private void removeFromRecord(long itemId) {
        CollectionTransactionLogRecord logRecord = (CollectionTransactionLogRecord) tx.get(name);
        int size = logRecord.removeOperation(itemId);
        if (size == 0) {
            tx.remove(name);
        }
    }

    public boolean remove(E e) {
        checkTransactionActive();
        checkObjectNotNull(e);

        final Data value = toData(e);
        final Iterator<CollectionItem> iterator = getCollection().iterator();
        long reservedItemId = -1;
        while (iterator.hasNext()) {
            final CollectionItem item = iterator.next();
            if (value.equals(item.getValue())) {
                reservedItemId = item.getItemId();
                break;
            }
        }

        Operation operation = new CollectionReserveRemoveOperation(name, reservedItemId, value, tx.getTxnId())
                .setPartitionId(partitionId)
                .setServiceName(getServiceName());
        InternalCompletableFuture<CollectionItem> f = invokeOnPartition(operation);
        CollectionItem item = f.join();
        if (item != null) {
            if (reservedItemId == item.getItemId()) {
                iterator.remove();
                removeFromRecord(reservedItemId);
                itemIdSet.remove(reservedItemId);
                return true;
            }
            if (!itemIdSet.add(item.getItemId())) {
                throw new TransactionException("Duplicate itemId: " + item.getItemId());
            }
            CollectionTxnRemoveOperation op = new CollectionTxnRemoveOperation(name, item.getItemId());
            putToRecord(op);
            return true;
        }
        return false;
    }

    public int size() {
        checkTransactionActive();

        Operation operation = new CollectionSizeOperation(name)
                .setPartitionId(partitionId)
                .setServiceName(getServiceName());

        InternalCompletableFuture<Integer> f = invokeOnPartition(operation);
        Integer size = f.join();
        return size + getCollection().size();
    }

    protected void checkTransactionActive() {
        if (!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    protected void checkObjectNotNull(Object o) {
        checkNotNull(o, "Object is null");
    }
}
