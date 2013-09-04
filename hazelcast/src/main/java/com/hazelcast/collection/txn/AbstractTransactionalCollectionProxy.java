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

package com.hazelcast.collection.txn;

import com.hazelcast.collection.CollectionItem;
import com.hazelcast.collection.CollectionSizeOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
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

/**
 * @ali 9/4/13
 */
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

    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }

    public boolean add(E e) {
        checkTransactionState();
        throwExceptionIfNull(e);
        final NodeEngine nodeEngine = getNodeEngine();
        final Data value = nodeEngine.toData(e);
        CollectionReserveAddOperation operation = new CollectionReserveAddOperation(name);
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(getServiceName(), operation, partitionId).build();
            Future<Long> f = invocation.invoke();
            Long itemId = f.get();
            if (itemId != null) {
                if (!itemIdSet.add(itemId)) {
                    throw new TransactionException("Duplicate itemId: " + itemId);
                }
                getCollection().add(new CollectionItem(null, itemId, value));
                tx.addTransactionLog(new CollectionTransactionLog(itemId, name, partitionId, getServiceName(), new CollectionTxnAddOperation(name, itemId, value)));
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
        while (iterator.hasNext()){
            final CollectionItem item = iterator.next();
            if (value.equals(item.getValue())){
                reservedItemId = item.getItemId();
                break;
            }
        }
        final CollectionReserveRemoveOperation operation = new CollectionReserveRemoveOperation(name, reservedItemId, value);
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(getServiceName(), operation, partitionId).build();
            Future<CollectionItem> f = invocation.invoke();
            CollectionItem item = f.get();
            if (item != null) {
                if (reservedItemId == item.getItemId()){
                    iterator.remove();
                    tx.removeTransactionLog(reservedItemId);
                    itemIdSet.remove(reservedItemId);
                    return true;
                }
                if (!itemIdSet.add(item.getItemId())) {
                    throw new TransactionException("Duplicate itemId: " + item.getItemId());
                }
                tx.addTransactionLog(new CollectionTransactionLog(item.getItemId(), name, partitionId, getServiceName(), new CollectionTxnRemoveOperation(name, item.getItemId())));
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
            Invocation invocation = getNodeEngine().getOperationService().createInvocationBuilder(getServiceName(), operation, partitionId).build();
            Future<Integer> f = invocation.invoke();
            Integer size = f.get();
            return size + getCollection().size();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private void checkTransactionState(){
        if(!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    private void throwExceptionIfNull(Object o) {
        if (o == null) {
            throw new NullPointerException("Object is null");
        }
    }

}
