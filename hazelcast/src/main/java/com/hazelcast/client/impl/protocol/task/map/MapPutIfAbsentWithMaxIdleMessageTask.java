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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapPutIfAbsentWithMaxIdleCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.concurrent.TimeUnit;

public class MapPutIfAbsentWithMaxIdleMessageTask
        extends AbstractMapPutWithMaxIdleMessageTask<MapPutIfAbsentWithMaxIdleCodec.RequestParameters> {

    public MapPutIfAbsentWithMaxIdleMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected MapPutIfAbsentWithMaxIdleCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapPutIfAbsentWithMaxIdleCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapPutIfAbsentWithMaxIdleCodec.encodeResponse(serializationService.toData(response));
    }

    protected Operation prepareOperation() {
        MapOperationProvider operationProvider = getMapOperationProvider(parameters.name);
        MapOperation op = operationProvider.createPutIfAbsentOperation(parameters.name, parameters.key,
                parameters.value, parameters.ttl, parameters.maxIdle);
        op.setThreadId(parameters.threadId);
        return op;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "putIfAbsent";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key, parameters.value, parameters.ttl, TimeUnit.MILLISECONDS,
                parameters.maxIdle, TimeUnit.MILLISECONDS};
    }
}
