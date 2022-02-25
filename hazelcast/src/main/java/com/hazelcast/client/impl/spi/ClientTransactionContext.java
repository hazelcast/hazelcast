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

import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalMap;

/**
 * Provides a context to perform transactional operations: beginning/committing transactions, but also retrieving
 * transactional data-structures like the {@link TransactionalMap}.
 *
 * Provides client instance and client connection proxies that need to be accessed for sending invocations.
 */
public interface ClientTransactionContext extends TransactionContext {

    HazelcastClientInstanceImpl getClient();

    ClientConnection getConnection();
}
