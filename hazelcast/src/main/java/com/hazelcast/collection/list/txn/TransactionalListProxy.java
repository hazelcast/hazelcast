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

import com.hazelcast.collection.list.ListService;
import com.hazelcast.core.TransactionalList;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionSupport;

/**
* @author ali 4/16/13
*/
public class TransactionalListProxy<E> extends AbstractDistributedObject<ListService> implements TransactionalList<E> {

    final String name;
    final TransactionSupport tx;

    public TransactionalListProxy(NodeEngine nodeEngine, ListService service, String name, TransactionSupport tx) {
        super(nodeEngine, service);
        this.name = name;
        this.tx = tx;
    }

    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }

    public boolean add(E e) {
        checkTransactionState();
        Data value = getNodeEngine().toData(e);
        return false;
    }

    public boolean remove(E e) {
        checkTransactionState();
        Data value = getNodeEngine().toData(e);
        return false;
    }

    public int size() {
        checkTransactionState();
        return 0;
    }

    protected void checkTransactionState(){
        if(!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    public String getServiceName() {
        return ListService.SERVICE_NAME;
    }
}
