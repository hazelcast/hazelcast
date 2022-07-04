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

package com.hazelcast.client.impl.proxy.txn;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.transaction.TransactionException;

import java.util.concurrent.Future;
import java.util.function.BiFunction;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Contains static method that is used from client transaction classes.
 */
public final class ClientTransactionUtil {

    private static final BiFunction<Throwable, String, RuntimeException> TRANSACTION_EXCEPTION_WRAPPER =
            (throwable, message) -> new TransactionException(message, throwable);

    private ClientTransactionUtil() {
    }

    /**
     * Handles the invocation exception for transactions so that users will not see internal exceptions.
     * <p>
     * More specifically IOException, because in case of a IO problem in ClientInvocation that send to a connection
     * sends IOException to user. This wraps that exception into a TransactionException.
     */
    public static ClientMessage invoke(ClientMessage request, Object objectName, HazelcastClientInstanceImpl client,
                                       Connection connection) {
        try {
            final ClientInvocation clientInvocation = new ClientInvocation(client, request, objectName, connection);
            final Future<ClientMessage> future = clientInvocation.invoke();
            return future.get();
        } catch (Exception e) {
            throw rethrow(e, TRANSACTION_EXCEPTION_WRAPPER);
        }
    }
}
