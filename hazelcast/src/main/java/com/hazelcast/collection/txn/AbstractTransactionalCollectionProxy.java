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

package com.hazelcast.collection.txn;

import com.hazelcast.collection.CollectionItem;
import com.hazelcast.collection.CollectionSizeOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.ExceptionUtil;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Future;

public abstract class AbstractTransactionalCollectionProxy<S extends RemoteService, E> extends AbstractDistributedObject<S> {

    protected final String name;
    protected final TransactionSupport tx;
    protected final int partitionId;
    protected final Set<Long> itemIdSet = new HashSet<Long>();

    public AbstractTransactionalCollectionProxy(String name, TransactionSupport tx, NodeEngine nodeEngine, S service) {
        super(nodeEngine, service);
        this.name = name;
        this.tx = tx;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    protected abstract Collection<CollectionItem> getCollection();

    @Override
    public String getName() {
        return name;
    }

    public boolean add(E e) {
        checkTransactionState();
        throwExceptionIfNull(e);
        final NodeEngine nodeEngine = getNodeEngine();
        final Data value = nodeEngine.toData(e);
        CollectionReserveAddOperation operation = new CollectionReserveAddOperation(name, tx.getTxnId(), null);
        try {
            Future<Long> f = nodeEngine.getOperationService().invokeOnPartition(getServiceName(), operation, partitionId);
            Long itemId = f.get();
            if (itemId != null) {
                if (!itemIdSet.add(itemId)) {
                    throw new TransactionException("Duplicate itemId: " + itemId);
                }
                getCollection().add(new CollectionItem(itemId, value));
                CollectionTxnAddOperation op = new CollectionTxnAddOperation(name, itemId, value);
                final String serviceName = getServiceName();
                final String txnId = tx.getTxnId();
                tx.addTransactionLog(new CollectionTransactionLog(itemId, name, partitionId, serviceName, txnId, op));
                return true;
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return false;
    }

    public boolean remove(E e) {
        checkTransactionState();
        throwExceptionIfNull(e);
        final NodeEngine nodeEngine = getNodeEngine();
        final Data value = nodeEngine.toData(e);
        final Iterator<CollectionItem> iterator = getCollection().iterator();
        long reservedItemId = -1;
        while (iterator.hasNext()) {
            final CollectionItem item = iterator.next();
            if (value.equals(item.getValue())) {
                reservedItemId = item.getItemId();
                break;
            }
        }
        final CollectionReserveRemoveOperation operation = new CollectionReserveRemoveOperation(
                name,
                reservedItemId,
                value,
                tx.getTxnId());
        try {
            final OperationService operationService = nodeEngine.getOperationService();
            Future<CollectionItem> f = operationService.invokeOnPartition(getServiceName(), operation, partitionId);
            CollectionItem item = f.get();
            if (item != null) {
                if (reservedItemId == item.getItemId()) {
                    iterator.remove();
                    tx.removeTransactionLog(reservedItemId);
                    itemIdSet.remove(reservedItemId);
                    return true;
                }
                if (!itemIdSet.add(item.getItemId())) {
                    throw new TransactionException("Duplicate itemId: " + item.getItemId());
                }
                CollectionTxnRemoveOperation op = new CollectionTxnRemoveOperation(name, item.getItemId());
                tx.addTransactionLog(new CollectionTransactionLog(
                        item.getItemId(),
                        name,
                        partitionId,
                        getServiceName(),
                        tx.getTxnId(),
                        op));
                return true;
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return false;
    }

    public int size() {
        checkTransactionState();
        CollectionSizeOperation operation = new CollectionSizeOperation(name);
        try {
            Future<Integer> f = getNodeEngine().getOperationService().invokeOnPartition(getServiceName(), operation, partitionId);
            Integer size = f.get();
            return size + getCollection().size();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected void checkTransactionState() {
        if (!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    protected void throwExceptionIfNull(Object o) {
        if (o == null) {
            throw new NullPointerException("Object is null");
        }
    }
}
