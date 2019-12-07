/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.client.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.function.Function;

abstract class AbstractJetMessageTask<P, R> extends AbstractInvocationMessageTask<P> {
    private final Function<ClientMessage, P> decoder;
    private final Function<R, ClientMessage> encoder;

    protected AbstractJetMessageTask(ClientMessage clientMessage, Node node, Connection connection,
                                     Function<ClientMessage, P> decoder, Function<R, ClientMessage> encoder) {
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
        return encoder.apply((R) o);
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

    protected <V> Data toData(V v) {
        return nodeEngine.getSerializationService().toData(v);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation operation) {
        Address masterAddress = nodeEngine.getMasterAddress();
        if (masterAddress == null) {
            throw new RetryableHazelcastException("master not yet known");
        }
        return nodeEngine.getOperationService().createInvocationBuilder(JetService.SERVICE_NAME,
                operation, masterAddress);
    }

    protected JetService getJetService() {
        return getService(JetService.SERVICE_NAME);
    }
}
