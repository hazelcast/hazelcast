/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.operation.CacheGetConfigOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheGetConfigCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

/**
 * This client request  specifically calls {@link CacheGetConfigOperation} on the server side.
 *
 * @see CacheGetConfigOperation
 */
public class CacheGetConfigMessageTask
        extends AbstractCacheMessageTask<CacheGetConfigCodec.RequestParameters> {
    public CacheGetConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new CacheGetConfigOperation(parameters.name, parameters.simpleName);
    }

    @Override
    protected CacheGetConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheGetConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        Data responseData = serializeCacheConfig(response);

        return CacheGetConfigCodec.encodeResponse(responseData);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
