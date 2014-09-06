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

package com.hazelcast.transaction;

import javax.transaction.xa.XAResource;

/**
 * Provides a context to do transactional operations; so beginning/committing transactions, but also retrieving
 * transactional data-structures like the {@link com.hazelcast.core.TransactionalMap}.
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
     * @throws IllegalStateException if no there is no active transaction.
     */
    void rollbackTransaction();

    /**
     * Gets the id that uniquely identifies the transaction.
     *
     * @return the transaction id.
     */
    String getTxnId();

    /**
     * Gets xaResource which will participate in XATransaction
     *
     * @return the xaResource.
     */
    XAResource getXaResource();

    /**
     * Indicates that related transaction is managed by XAResource
     *
     * @return true if related transaction is managed by XAResource, false otherwise
     */
    boolean isXAManaged();
}
