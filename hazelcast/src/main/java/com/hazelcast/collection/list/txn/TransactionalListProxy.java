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

package com.hazelcast.collection.list.txn;

import com.hazelcast.collection.CollectionItem;
import com.hazelcast.collection.CollectionSizeOperation;
import com.hazelcast.collection.list.ListService;
import com.hazelcast.collection.txn.*;
import com.hazelcast.core.TransactionalList;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.ExceptionUtil;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Future;

/**
* @author ali 4/16/13
*/
public class TransactionalListProxy<E> extends AbstractDistributedObject<ListService> implements TransactionalList<E> {

    private final String name;
    private final TransactionSupport tx;
    private final int partitionId;
    private final Set<Long> itemIdSet = new HashSet<Long>();
    private final LinkedList<CollectionItem> list = new LinkedList<CollectionItem>();

    public TransactionalListProxy(NodeEngine nodeEngine, ListService service, String name, TransactionSupport tx) {
        super(nodeEngine, service);
        this.name = name;
        this.tx = tx;
        partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

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
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(ListService.SERVICE_NAME, operation, partitionId).build();
            Future<Long> f = invocation.invoke();
            Long itemId = f.get();
            if (itemId != null) {
                if (!itemIdSet.add(itemId)) {
                    throw new TransactionException("Duplicate itemId: " + itemId);
                }
                list.add(new CollectionItem(null, itemId, value));
                tx.addTransactionLog(new CollectionTransactionLog(itemId, name, partitionId, ListService.SERVICE_NAME, new CollectionTxnAddOperation(name, itemId, value)));
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
        final Iterator<CollectionItem> iterator = list.iterator();
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
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(ListService.SERVICE_NAME, operation, partitionId).build();
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
                tx.addTransactionLog(new CollectionTransactionLog(item.getItemId(), name, partitionId, ListService.SERVICE_NAME, new CollectionTxnRemoveOperation(name, item.getItemId())));
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
            Invocation invocation = getNodeEngine().getOperationService().createInvocationBuilder(ListService.SERVICE_NAME, operation, partitionId).build();
            Future<Integer> f = invocation.invoke();
            Integer size = f.get();
            return size + list.size();
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

    public String getServiceName() {
        return ListService.SERVICE_NAME;
    }
}
