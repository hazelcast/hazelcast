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
import com.hazelcast.collection.set.SetService;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.ExceptionUtil;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.Future;

public class TransactionalSetProxy<E> extends AbstractTransactionalCollectionProxy<SetService, E>
        implements TransactionalSet<E> {

    private final HashSet<CollectionItem> set = new HashSet<CollectionItem>();

    public TransactionalSetProxy(String name, TransactionSupport tx, NodeEngine nodeEngine, SetService service) {
        super(name, tx, nodeEngine, service);
    }

    @Override
    public boolean add(E e) {
        checkTransactionState();
        throwExceptionIfNull(e);
        final NodeEngine nodeEngine = getNodeEngine();
        final Data value = nodeEngine.toData(e);
        if (!getCollection().add(new CollectionItem(-1, value))) {
            return false;
        }
        CollectionReserveAddOperation operation = new CollectionReserveAddOperation(name, tx.getTxnId(), value);
        try {
            Future<Long> f = nodeEngine.getOperationService().invokeOnPartition(getServiceName(), operation, partitionId);
            Long itemId = f.get();
            if (itemId != null) {
                if (!itemIdSet.add(itemId)) {
                    throw new TransactionException("Duplicate itemId: " + itemId);
                }
                CollectionTxnAddOperation op = new CollectionTxnAddOperation(name, itemId, value);
                final String txnId = tx.getTxnId();
                final String serviceName = getServiceName();
                tx.addTransactionLog(new CollectionTransactionLog(itemId, name, partitionId, serviceName, txnId, op));
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
