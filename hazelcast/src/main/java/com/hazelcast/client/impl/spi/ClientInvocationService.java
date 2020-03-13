/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.connection.nio.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.cluster.Member;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * Invocation service for Hazelcast clients.
 *
 * Allows remote invocations on different targets like {@link ClientConnection},
 * partition owners or {@link Member} based targets.
 */
public interface ClientInvocationService {

    boolean invokeOnConnection(ClientInvocation invocation, ClientConnection connection);

    boolean invokeOnPartitionOwner(ClientInvocation invocation, int partitionId);

    boolean invokeOnRandomTarget(ClientInvocation invocation);

    boolean invokeOnTarget(ClientInvocation invocation, UUID uuid);

    boolean isRedoOperation();

    Consumer<ClientMessage> getResponseHandler();
}
