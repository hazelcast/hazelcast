/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalList;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalMultiMap;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.TransactionalQueue;
import com.hazelcast.transaction.TransactionalSet;

import java.util.UUID;

/**
 * Holder wrapping a Hazelcast TransactionContext.
 * <p>
 * HazelcastTransactionManager binds instances of this class to the thread, for a given HazelcastInstance.
 *
 * @author Balint Krivan
 * @author Sergey Bespalov
 * @see HazelcastTransactionManager
 */
class TransactionContextHolder implements TransactionContext {

    private final TransactionContext transactionContext;
    private boolean transactionActive;

    TransactionContextHolder(TransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    public void beginTransaction() {
        transactionContext.beginTransaction();
        transactionActive = true;
    }

    public void commitTransaction() throws TransactionException {
        try {
            transactionContext.commitTransaction();
        } finally {
            transactionActive = false;
        }
    }

    public void rollbackTransaction() {
        try {
            transactionContext.rollbackTransaction();
        } finally {
            transactionActive = false;
        }
    }

    public boolean isTransactionActive() {
        return transactionActive;
    }

    public <K, V> TransactionalMap<K, V> getMap(String name) {
        return transactionContext.getMap(name);
    }

    public <E> TransactionalQueue<E> getQueue(String name) {
        return transactionContext.getQueue(name);
    }

    public <K, V> TransactionalMultiMap<K, V> getMultiMap(String name) {
        return transactionContext.getMultiMap(name);
    }

    public void suspendTransaction() {
        transactionContext.suspendTransaction();
    }

    public void resumeTransaction() {
        transactionContext.resumeTransaction();
    }

    public UUID getTxnId() {
        return transactionContext.getTxnId();
    }

    public <E> TransactionalList<E> getList(String name) {
        return transactionContext.getList(name);
    }

    public <E> TransactionalSet<E> getSet(String name) {
        return transactionContext.getSet(name);
    }

    public <T extends TransactionalObject> T getTransactionalObject(String serviceName, String name) {
        return transactionContext.getTransactionalObject(serviceName, name);
    }

}
