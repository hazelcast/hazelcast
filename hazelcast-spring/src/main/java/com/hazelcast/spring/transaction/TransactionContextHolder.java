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

import com.hazelcast.transaction.TransactionContext;

/**
 * Holder wrapping a Hazelcast TransactionContext.
 * <p>
 * HazelcastTransactionManager binds instances of this class to the thread, for a given HazelcastInstance.
 *
 * @author Balint Krivan
 * @see HazelcastTransactionManager
 */
class TransactionContextHolder {

    private final TransactionContext transactionContext;
    private boolean transactionActive;

    TransactionContextHolder(TransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    public boolean isTransactionActive() {
        return transactionActive;
    }

    public TransactionContext getContext() {
        return transactionContext;
    }

    /**
     * @see TransactionContext#beginTransaction()
     */
    public void beginTransaction() {
        transactionContext.beginTransaction();
        transactionActive = true;
    }

    public void clear() {
        transactionActive = false;
    }
}
