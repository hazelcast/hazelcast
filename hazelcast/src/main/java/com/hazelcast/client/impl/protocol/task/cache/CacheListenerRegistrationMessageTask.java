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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheListenerRegistrationOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheListenerRegistrationCodec;
import com.hazelcast.client.impl.protocol.task.AbstractTargetMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import java.security.Permission;
import java.util.UUID;

/**
 * This client request  specifically calls {@link CacheListenerRegistrationOperation} on the server side.
 *
 * @see CacheListenerRegistrationOperation
 */
public class CacheListenerRegistrationMessageTask
        extends AbstractTargetMessageTask<CacheListenerRegistrationCodec.RequestParameters> {

    public CacheListenerRegistrationMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected UUID getTargetUuid() {
        return parameters.uuid;
    }

    @Override
    protected Operation prepareOperation() {
        CacheEntryListenerConfiguration conf = nodeEngine.toObject(parameters.listenerConfig);
        return new CacheListenerRegistrationOperation(parameters.name, conf, parameters.shouldRegister);
    }

    @Override
    protected CacheListenerRegistrationCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheListenerRegistrationCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheListenerRegistrationCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
