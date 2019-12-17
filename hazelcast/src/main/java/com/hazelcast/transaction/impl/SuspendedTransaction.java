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
 * Base class for suspended {@link Transaction} implementations.
 *
 * @author Sergey Bespalov
 */
public abstract class SuspendedTransaction implements Transaction {

    private final Transaction transaction;

    public SuspendedTransaction(Transaction transaction) {
        this.transaction = transaction;
        suspend(transaction);
    }

    protected Transaction getTransaction() {
        return transaction;
    }

    public void begin() throws IllegalStateException {
        throw new IllegalStateException("Transaction suspended.");
    }

    public void prepare() throws TransactionException {
        throw new IllegalStateException("Transaction suspended.");
    }

    public void commit() throws TransactionException, IllegalStateException {
        throw new IllegalStateException("Transaction suspended.");
    }

    public void rollback() throws IllegalStateException {
        throw new IllegalStateException("Transaction suspended.");
    }

    public UUID getTxnId() {
        return transaction.getTxnId();
    }

    public State getState() {
        return transaction.getState();
    }

    public long getTimeoutMillis() {
        return transaction.getTimeoutMillis();
    }

    public void add(TransactionLogRecord record) {
        throw new IllegalStateException("Transaction suspended.");
    }

    public void remove(Object key) {
        throw new IllegalStateException("Transaction suspended.");
    }

    public TransactionLogRecord get(Object key) {
        throw new IllegalStateException("Transaction suspended.");
    }

    public UUID getOwnerUuid() {
        return transaction.getOwnerUuid();
    }

    public boolean isOriginatedFromClient() {
        return transaction.isOriginatedFromClient();
    }

    public TransactionType getTransactionType() {
        return transaction.getTransactionType();
    }

    protected abstract void suspend(Transaction transaction);

    public abstract void resume();

}
