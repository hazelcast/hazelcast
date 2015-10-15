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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;

/**
 * ClientInvocation that is specifically dealing with invocations that registers listener to server
 */
public class ClientListenerInvocation extends ClientInvocation {

    private final EventHandler handler;

    public ClientListenerInvocation(HazelcastClientInstanceImpl client, EventHandler handler,
                                    ClientMessage clientMessage) {
        super(client, clientMessage, UNASSIGNED_PARTITION, null, null);
        this.handler = handler;
        clientInvocationFuture = new ClientListenerFuture(this, client, clientMessage, handler);
    }

    public ClientListenerInvocation(HazelcastClientInstanceImpl client, EventHandler handler,
                                    ClientMessage clientMessage, int partitionId) {
        super(client, clientMessage, partitionId, null, null);
        this.handler = handler;
        clientInvocationFuture = new ClientListenerFuture(this, client, clientMessage, handler);
    }

    public ClientListenerInvocation(HazelcastClientInstanceImpl client, EventHandler handler,
                                    ClientMessage clientMessage, Address address) {
        super(client, clientMessage, UNASSIGNED_PARTITION, address, null);
        this.handler = handler;
        clientInvocationFuture = new ClientListenerFuture(this, client, clientMessage, handler);
    }

    public ClientListenerInvocation(HazelcastClientInstanceImpl client, EventHandler handler,
                                    ClientMessage clientMessage, Connection connection) {
        super(client, clientMessage, UNASSIGNED_PARTITION, null, connection);
        this.handler = handler;
        clientInvocationFuture = new ClientListenerFuture(this, client, clientMessage, handler);
    }

    public EventHandler getHandler() {
        return handler;
    }

    @Override
    protected void beforeRetry() {
        handler.beforeListenerRegister();
    }

    @Override
    protected boolean shouldRetry() {
        return true;
    }
}
