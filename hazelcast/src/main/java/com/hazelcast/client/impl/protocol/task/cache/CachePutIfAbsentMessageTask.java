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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.operation.CachePutIfAbsentOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CachePutIfAbsentCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;

/**
 * This client request  specifically calls {@link CachePutIfAbsentOperation} on the server side.
 *
 * @see CachePutIfAbsentOperation
 */
public class CachePutIfAbsentMessageTask
        extends AbstractCacheMessageTask<CachePutIfAbsentCodec.RequestParameters> {

    public CachePutIfAbsentMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        CacheOperationProvider operationProvider = getOperationProvider(parameters.name);
        ExpiryPolicy expiryPolicy = (ExpiryPolicy) nodeEngine.toObject(parameters.expiryPolicy);
        return operationProvider
                .createPutIfAbsentOperation(parameters.key, parameters.value, expiryPolicy, parameters.completionId);
    }

    @Override
    protected CachePutIfAbsentCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CachePutIfAbsentCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CachePutIfAbsentCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
