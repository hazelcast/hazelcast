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
import com.hazelcast.cache.impl.operation.CacheGetOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheGetCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.security.Permission;

/**
 * This client request  specifically calls {@link CacheGetOperation} on the server side.
 *
 * @see CacheGetOperation
 */
public class CacheGetMessageTask
        extends AbstractCacheMessageTask<CacheGetCodec.RequestParameters> {

    public CacheGetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        CacheOperationProvider operationProvider = getOperationProvider(parameters.name);
        ExpiryPolicy expiryPolicy = (ExpiryPolicy) nodeEngine.toObject(parameters.expiryPolicy);
        return operationProvider.createGetOperation(parameters.key, expiryPolicy);
    }

    @Override
    protected CacheGetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheGetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheGetCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        if (parameters.expiryPolicy == null) {
            return new Object[]{parameters.key};
        }
        return new Object[]{parameters.key, parameters.expiryPolicy};
    }

    @Override
    public String getMethodName() {
        return "get";
    }
}
