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

package com.hazelcast.client.impl.spi;

import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import javax.annotation.Nonnull;
import javax.transaction.xa.Xid;

/**
 * Manages the execution of client transactions and provides {@link TransactionContext}s.
 *
 * Client equivalent of {@link com.hazelcast.transaction.TransactionManagerService}.
 */
public interface ClientTransactionManagerService {

    <T> T executeTransaction(@Nonnull TransactionalTask<T> task);

    <T> T executeTransaction(@Nonnull TransactionOptions options, @Nonnull TransactionalTask<T> task);

    TransactionContext newTransactionContext();

    TransactionContext newTransactionContext(@Nonnull TransactionOptions options);

    /**
     * @param xid              branch qualifier
     * @param timeoutInSeconds transaction timeout in seconds
     * @return a {@link TransactionContext} for the supplied branch qualifier
     */
    TransactionContext newXATransactionContext(Xid xid, int timeoutInSeconds);

    String getClusterName();
}
