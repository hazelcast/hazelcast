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

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.EventHandler;

/**
 * ClientInvocationFuture that dealing with event responses
 */
public class ClientListenerFuture extends ClientInvocationFuture {

    private final EventHandler handler;
    private final ClientListenerServiceImpl clientListenerService;
    private final ClientMessageDecoder clientMessageDecoder;

    public ClientListenerFuture(ClientInvocation invocation, HazelcastClientInstanceImpl client,
                                ClientMessage clientMessage, EventHandler handler,
                                ClientMessageDecoder clientMessageDecoder) {
        super(invocation, client, clientMessage);
        this.handler = handler;
        this.clientListenerService = (ClientListenerServiceImpl) client.getListenerService();
        this.clientMessageDecoder = clientMessageDecoder;
    }

    @Override
    boolean shouldSetResponse(Object response) {
        if (response instanceof Throwable) {
            return true;
        }

        handler.onListenerRegister();

        if (this.response != null) {
            ClientMessage uuidMessage = (ClientMessage) this.response;
            ClientMessage copyFlyweight = ClientMessage.createForDecode(uuidMessage.buffer(), 0);

            String uuid = clientMessageDecoder.decodeClientMessage((ClientMessage) copyFlyweight);
            String alias = clientMessageDecoder.decodeClientMessage((ClientMessage) response);
            clientListenerService.reRegisterListener(uuid, alias, clientMessage.getCorrelationId());
            return false;
        }
        return true;
    }

}
