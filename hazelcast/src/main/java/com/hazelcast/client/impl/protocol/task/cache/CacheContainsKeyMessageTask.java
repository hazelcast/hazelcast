/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheContainsKeyOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheContainsKeyCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;

/**
 * This client request  specifically calls {@link CacheContainsKeyOperation} on the server side.
 *
 * @see CacheContainsKeyOperation
 */
public class CacheContainsKeyMessageTask
        extends AbstractCacheMessageTask<CacheContainsKeyCodec.RequestParameters> {

    public CacheContainsKeyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        CacheOperationProvider operationProvider = getOperationProvider(parameters.name);
        return operationProvider.createContainsKeyOperation(parameters.key);
    }

    @Override
    protected CacheContainsKeyCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheContainsKeyCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheContainsKeyCodec.encodeResponse((Boolean) response);
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key};
    }

    @Override
    public String getMethodName() {
        return "containsKey";
    }
}
