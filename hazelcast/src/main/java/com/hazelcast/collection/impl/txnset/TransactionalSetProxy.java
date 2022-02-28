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

package com.hazelcast.collection.impl.txnset;

import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.collection.impl.txncollection.AbstractTransactionalCollectionProxy;
import com.hazelcast.collection.impl.txncollection.operations.CollectionReserveAddOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionTxnAddOperation;
import com.hazelcast.transaction.TransactionalSet;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.Future;

import static com.hazelcast.collection.impl.collection.CollectionContainer.INVALID_ITEM_ID;

public class TransactionalSetProxy<E>
        extends AbstractTransactionalCollectionProxy<SetService, E>
        implements TransactionalSet<E> {

    private final HashSet<CollectionItem> set = new HashSet<CollectionItem>();

    public TransactionalSetProxy(String name, Transaction tx, NodeEngine nodeEngine, SetService service) {
        super(name, tx, nodeEngine, service);
    }

    @Override
    public boolean add(E e) {
        checkTransactionActive();
        checkObjectNotNull(e);

        Data value = getNodeEngine().toData(e);
        if (!getCollection().add(new CollectionItem(INVALID_ITEM_ID, value))) {
            return false;
        }

        CollectionReserveAddOperation operation = new CollectionReserveAddOperation(name, tx.getTxnId(), value);
        try {
            Future<Long> future = operationService.invokeOnPartition(getServiceName(), operation, partitionId);
            Long itemId = future.get();
            if (itemId != null) {
                if (!itemIdSet.add(itemId)) {
                    throw new TransactionException("Duplicate itemId: " + itemId);
                }
                CollectionTxnAddOperation op = new CollectionTxnAddOperation(name, itemId, value);
                putToRecord(op);
                return true;
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return false;
    }

    @Override
    public String getServiceName() {
        return SetService.SERVICE_NAME;
    }

    @Override
    protected Collection<CollectionItem> getCollection() {
        return set;
    }
}
