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

package com.hazelcast.transaction.impl;

import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions.TransactionType;

import java.util.UUID;

/**
 * This wrapper helps to manage the suspended transaction within {@link TransactionContextImpl}.
 *
 * @author Sergey Bespalov
 *
 * @see SuspendedTransaction
 */
class TransactionWrapper implements Transaction {

    private Transaction transaction;

    TransactionWrapper(Transaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public void begin() throws IllegalStateException {
        transaction.begin();
    }

    @Override
    public void prepare() throws TransactionException {
        transaction.prepare();
    }

    @Override
    public void commit() throws TransactionException, IllegalStateException {
        transaction.commit();
    }

    @Override
    public void rollback() throws IllegalStateException {
        transaction.rollback();
    }

    @Override
    public UUID getTxnId() {
        return transaction.getTxnId();
    }

    @Override
    public State getState() {
        return transaction.getState();
    }

    @Override
    public long getTimeoutMillis() {
        return transaction.getTimeoutMillis();
    }

    @Override
    public void add(TransactionLogRecord record) {
        transaction.add(record);
    }

    @Override
    public void remove(Object key) {
        transaction.remove(key);
    }

    @Override
    public TransactionLogRecord get(Object key) {
        return transaction.get(key);
    }

    @Override
    public UUID getOwnerUuid() {
        return transaction.getOwnerUuid();
    }

    @Override
    public boolean isOriginatedFromClient() {
        return transaction.isOriginatedFromClient();
    }

    @Override
    public TransactionType getTransactionType() {
        return transaction.getTransactionType();
    }

    void set(Transaction transaction) {
        this.transaction = transaction;
    }

    <T extends Transaction> T getTransactionOfRequiredType(Class<T> type) {
        if (transaction == null || !type.isAssignableFrom(transaction.getClass())) {
            throw new IllegalStateException("Invalid transaction state.");
        }
        return type.cast(transaction);
    }

}
