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

package com.hazelcast.core;

import com.hazelcast.transaction.TransactionException;

/**
 * Hazelcast transaction interface.
 */
public interface Transaction {

    public enum State {
        NO_TXN(-1),
        ACTIVE(0),
        PREPARING(1),
        PREPARED(2),
        COMMITTING(3),
        COMMITTED(4),
        COMMIT_FAILED(5),
        ROLLING_BACK(6),
        ROLLED_BACK(7);

        final int value;

        private State(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /**
     * Begins the transaction.
     *
     * @throws IllegalStateException if transaction is already began
     */
    void begin() throws IllegalStateException;

    /**
     * Commits the transaction.
     *
     * @throws IllegalStateException if transaction didn't begin.
     */
    void commit() throws TransactionException, IllegalStateException;

    /**
     * Rolls back the transaction.
     *
     * @throws IllegalStateException if transaction didn't begin.
     */
    void rollback() throws IllegalStateException;

    /**
     * Returns the status of the transaction.
     *
     * @return the status
     */
    int getStatus();

    /**
     * Returns the state of the transaction.
     *
     * @return the state
     */
    State getState();

    /**
     * @param seconds
     */
    void setTransactionTimeout(int seconds);
}
