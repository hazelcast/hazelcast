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

package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;


/**
 * Context to be used by TemplateHandlers to handle client messages
 */
public class MessageHandlerContext {

    private final NodeEngineImpl nodeEngine;
    private final ClientEngine clientEngine;
    private final SerializationService serializationService;
    private final ClientEndpoint clientEndpoint;
    private final OperationService operationService;
    private final ClientMessage clientMessage;
    private final MessageHandlerParameters parameters;

    public MessageHandlerContext(NodeEngineImpl nodeEngine, ClientEngine clientEngine,
                                 SerializationService serializationService,
                                 ClientEndpoint clientEndpoint, OperationService operationService,
                                 ClientMessage clientMessage, MessageHandler messageHandler) {
        this.nodeEngine = nodeEngine;
        this.clientEngine = clientEngine;
        this.serializationService = serializationService;
        this.clientEndpoint = clientEndpoint;
        this.operationService = operationService;
        this.clientMessage = clientMessage;
        this.parameters = initParameters(messageHandler);
    }

    private MessageHandlerParameters initParameters(MessageHandler messageHandler) {
        ClientMessageReaderImpl reader = new ClientMessageReaderImpl(clientMessage, serializationService);

        MessageHandlerParameters parameters = messageHandler.createParameters();
        if (parameters != null) {
            parameters.readMessage(reader);
            return parameters;
        }
        return null;
    }


    public ClientEngine getClientEngine() {
        return clientEngine;
    }

    public SerializationService getSerializationService() {
        return serializationService;
    }

    public ClientEndpoint getClientEndpoint() {
        return clientEndpoint;
    }

    public OperationService getOperationService() {
        return operationService;
    }

    public ClientMessage getClientMessage() {
        return clientMessage;
    }

    public <S> S getService(String serviceName) {
        Object service = nodeEngine.getService(serviceName);
        if (service == null) {
            if (nodeEngine.isActive()) {
                throw new IllegalArgumentException("No service registered with name: " + serviceName);
            }
            throw new HazelcastInstanceNotActiveException();
        }
        return (S) service;
    }

    public <P extends MessageHandlerParameters> P getParameters() {
        return (P) parameters;
    }
}

