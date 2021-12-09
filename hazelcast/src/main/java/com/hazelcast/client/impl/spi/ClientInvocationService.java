/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.invocation.ClientInvocation;
import com.hazelcast.client.impl.spi.invocation.ClientInvocationFuture;

import java.util.Map;
import java.util.UUID;

public interface ClientInvocationService {

    //TODO sancar introduce enums for URGENT and ALLOW_RETRY
    int USE_GLOBAL_INVOCATION_TIMEOUT_MILLIS = -1;
    boolean DEFAULT_ALLOW_RETRY_ON_RANDOM = true;
    boolean DEFAULT_URGENT = false;

    Map<Long, ClientInvocation> getUnmodifiableInvocations();

    boolean isBackupAckToClientEnabled();

    ClientInvocationFuture invokeOnPartition(ClientMessage clientMessage,
                                             Object objectName, int partitionId,
                                             boolean urgent,
                                             boolean allowRetryOnRandom,
                                             long invocationTimeoutMillis);

    default ClientInvocationFuture invokeOnPartition(ClientMessage clientMessage, Object objectName,
                                                     int partitionId, boolean urgent) {
        return invokeOnPartition(clientMessage, objectName, partitionId, urgent, DEFAULT_ALLOW_RETRY_ON_RANDOM,
                USE_GLOBAL_INVOCATION_TIMEOUT_MILLIS);
    }

    default ClientInvocationFuture invokeOnPartition(ClientMessage clientMessage, Object objectName, int partitionId) {
        return invokeOnPartition(clientMessage, objectName, partitionId, DEFAULT_URGENT, DEFAULT_ALLOW_RETRY_ON_RANDOM,
                USE_GLOBAL_INVOCATION_TIMEOUT_MILLIS);
    }

    ClientInvocationFuture invokeOnConnection(ClientMessage request, EventHandler handler, Object objectName,
                                              ClientConnection connection, boolean urgent,
                                              boolean allowRetryOnRandom, long invocationTimeoutMillis);

    default ClientInvocationFuture invokeOnConnection(ClientMessage clientMessage,
                                                      EventHandler handler, Object objectName,
                                                      ClientConnection connection,
                                                      boolean urgent) {
        return invokeOnConnection(clientMessage, handler, objectName, connection, urgent,
                DEFAULT_ALLOW_RETRY_ON_RANDOM, USE_GLOBAL_INVOCATION_TIMEOUT_MILLIS);
    }

    default ClientInvocationFuture invokeOnConnection(ClientMessage clientMessage,
                                                      Object objectName,
                                                      ClientConnection connection,
                                                      boolean urgent) {
        return invokeOnConnection(clientMessage, null, objectName, connection, urgent,
                DEFAULT_ALLOW_RETRY_ON_RANDOM, USE_GLOBAL_INVOCATION_TIMEOUT_MILLIS);
    }


    default ClientInvocationFuture invokeOnConnection(ClientMessage clientMessage, Object objectName,
                                                      ClientConnection connection) {
        return invokeOnConnection(clientMessage, null, objectName, connection, DEFAULT_URGENT,
                DEFAULT_ALLOW_RETRY_ON_RANDOM, USE_GLOBAL_INVOCATION_TIMEOUT_MILLIS);
    }

    ClientInvocationFuture invokeOnRandom(ClientMessage clientMessage,
                                          Object objectName,
                                          boolean urgent,
                                          boolean allowRetryOnRandom,
                                          long invocationTimeoutMillis);


    default ClientInvocationFuture invokeOnRandom(ClientMessage clientMessage, Object objectName) {
        return invokeOnRandom(clientMessage, objectName, DEFAULT_URGENT, DEFAULT_ALLOW_RETRY_ON_RANDOM,
                USE_GLOBAL_INVOCATION_TIMEOUT_MILLIS);
    }

    default ClientInvocationFuture invokeOnRandom(ClientMessage clientMessage, Object objectName, boolean urgent) {
        return invokeOnRandom(clientMessage, objectName, urgent, DEFAULT_ALLOW_RETRY_ON_RANDOM,
                USE_GLOBAL_INVOCATION_TIMEOUT_MILLIS);
    }

    ClientInvocationFuture invokeOnMember(ClientMessage clientMessage,
                                          Object objectName, UUID uuid,
                                          boolean urgent,
                                          boolean allowRetryOnRandom,
                                          long invocationTimeoutMillis);

    default ClientInvocationFuture invokeOnMember(ClientMessage clientMessage, Object objectName, UUID uuid) {
        return invokeOnMember(clientMessage, objectName, uuid, DEFAULT_URGENT, DEFAULT_ALLOW_RETRY_ON_RANDOM,
                USE_GLOBAL_INVOCATION_TIMEOUT_MILLIS);
    }

    void handleResponseMessage(ClientMessage message);
}
