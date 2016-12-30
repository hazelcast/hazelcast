/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.impl.EngineContext;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;

import java.security.Permission;
import java.util.function.Function;

abstract class AbstractJetMessageTask<P> extends AbstractInvocationMessageTask<P> implements ExecutionCallback {
    private final Function<ClientMessage, P> decoder;
    private final Function<Object, ClientMessage> encoder;

    protected AbstractJetMessageTask(ClientMessage clientMessage, Node node, Connection connection,
                                     Function<ClientMessage, P> decoder, Function<Object, ClientMessage> encoder) {
        super(clientMessage, node, connection);

        this.decoder = decoder;
        this.encoder = encoder;
    }

    @Override
    public String getServiceName() {
        return JetService.SERVICE_NAME;
    }


    @Override
    protected final P decodeClientMessage(ClientMessage clientMessage) {
        return decoder.apply(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object o) {
        return encoder.apply(o);
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    protected <V> V toObject(Data data) {
        return nodeEngine.getSerializationService().toObject(data);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation operation) {
        return nodeEngine.getOperationService().createInvocationBuilder(JetService.SERVICE_NAME,
                operation, nodeEngine.getThisAddress());
    }

    protected EngineContext getEngineContext() {
        JetService service = getService(JetService.SERVICE_NAME);
        return service.getEngineContext();
    }


}
