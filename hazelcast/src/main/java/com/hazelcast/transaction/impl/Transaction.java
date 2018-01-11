/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

public interface Transaction {

    void begin() throws IllegalStateException;

    void prepare() throws TransactionException;

    void commit() throws TransactionException, IllegalStateException;

    void rollback() throws IllegalStateException;

    String getTxnId();

    State getState();

    long getTimeoutMillis();

    void add(TransactionLogRecord record);

    void remove(Object key);

    TransactionLogRecord get(Object key);

    String getOwnerUuid();

    boolean isOriginatedFromClient();

    TransactionType getTransactionType();

    enum State {
        NO_TXN,
        ACTIVE,
        PREPARING,
        PREPARED,
        COMMITTING,
        COMMITTED,
        COMMIT_FAILED,
        ROLLING_BACK,
        ROLLED_BACK
    }
}
