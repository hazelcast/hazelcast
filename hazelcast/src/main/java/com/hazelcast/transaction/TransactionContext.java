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

package com.hazelcast.transaction;

import java.util.UUID;

/**
 * Provides a context to perform transactional operations: beginning/committing transactions, but also retrieving
 * transactional data-structures like the {@link TransactionalMap}.
 * Any method accessed through TransactionContext interface can throw TransactionException if transaction is
 * no longer valid and rolled back.
 */
public interface TransactionContext extends TransactionalTaskContext {

    /**
     * Begins a transaction.
     *
     * @throws IllegalStateException if a transaction already is active.
     */
    void beginTransaction();

    /**
     * Commits a transaction.
     *
     * @throws TransactionException if no transaction is active or the transaction could not be committed.
     */
    void commitTransaction() throws TransactionException;

    /**
     * Rollback of the current transaction.
     *
     * @throws IllegalStateException if there is no active transaction.
     */
    void rollbackTransaction();

    /**
     * Gets the ID that uniquely identifies the transaction.
     *
     * @return the transaction ID
     */
    UUID getTxnId();
}
