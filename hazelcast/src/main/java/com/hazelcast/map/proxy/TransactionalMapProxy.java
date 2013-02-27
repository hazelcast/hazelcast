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

package com.hazelcast.map.proxy;

import com.hazelcast.core.Transaction;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.map.MapService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.TransactionImpl;
import com.hazelcast.transaction.TransactionalObject;

/**
 * @mdogan 2/26/13
 */
public class TransactionalMapProxy<K,V> extends ObjectMapProxy<K,V> implements TransactionalMap<K,V>, TransactionalObject {

    private TransactionImpl transaction;

    public TransactionalMapProxy(String name, MapService mapService, NodeEngine nodeEngine) {
        super(name, mapService, nodeEngine);
    }

    public void setTransaction(TransactionImpl transaction) {
        this.transaction = transaction;
    }

    protected final String attachTxnParticipant(int partitionId) {
        if (transaction.getState() != Transaction.State.ACTIVE) {
            throw new IllegalStateException("Transaction is not active!");
        }
        transaction.attachParticipant(partitionId, getServiceName());
        return transaction.getTxnId();
    }

    protected final String getCurrentTransactionId() {
        if (transaction.getState() != Transaction.State.ACTIVE) {
            throw new IllegalStateException("Transaction is not active!");
        }
        return transaction.getTxnId();
    }
}
