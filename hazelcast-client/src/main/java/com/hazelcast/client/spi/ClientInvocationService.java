/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;

import java.io.IOException;

/**
 * @author mdogan 5/16/13
 */
public interface ClientInvocationService {

    void start();

    void invokeOnConnection(ClientInvocation invocation, ClientConnection connection) throws IOException;

    void invokeOnPartitionOwner(ClientInvocation invocation, int partitionId) throws IOException;

    void invokeOnRandomTarget(ClientInvocation invocation) throws IOException;

    void invokeOnTarget(ClientInvocation invocation, Address target) throws IOException;

    boolean isRedoOperation();

    void shutdown();

    void handleClientMessage(ClientMessage message, Connection connection);

    void cleanConnectionResources(ClientConnection connection);

}
