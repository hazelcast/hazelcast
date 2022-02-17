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

package com.hazelcast.spring.transaction;

import com.hazelcast.transaction.TransactionalList;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalMultiMap;
import com.hazelcast.transaction.TransactionalQueue;
import com.hazelcast.transaction.TransactionalSet;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.TransactionalTaskContext;

/**
 * {@link TransactionalTaskContext} proxying implementation to make the one created by {@link HazelcastTransactionManager}
 * available to actual business logic. Useful when the transaction is managed declaratively using
 * {@link org.springframework.transaction.annotation.Transactional @Transactional} annotations with AOP.
 *
 * @author Balint Krivan
 * @see HazelcastTransactionManager
 */
public class ManagedTransactionalTaskContext implements TransactionalTaskContext {

    private final HazelcastTransactionManager hzTxMgr;

    public ManagedTransactionalTaskContext(HazelcastTransactionManager hzTxMgr) {
        this.hzTxMgr = hzTxMgr;
    }

    @Override
    public <K, V> TransactionalMap<K, V> getMap(String name) {
        return transactionContext().getMap(name);
    }

    @Override
    public <E> TransactionalQueue<E> getQueue(String name) {
        return transactionContext().getQueue(name);
    }

    @Override
    public <K, V> TransactionalMultiMap<K, V> getMultiMap(String name) {
        return transactionContext().getMultiMap(name);
    }

    @Override
    public <E> TransactionalList<E> getList(String name) {
        return transactionContext().getList(name);
    }

    @Override
    public <E> TransactionalSet<E> getSet(String name) {
        return transactionContext().getSet(name);
    }

    @Override
    public <T extends TransactionalObject> T getTransactionalObject(String serviceName, String name) {
        return transactionContext().getTransactionalObject(serviceName, name);
    }

    private TransactionContext transactionContext() {
        return hzTxMgr.getTransactionContext();
    }
}
