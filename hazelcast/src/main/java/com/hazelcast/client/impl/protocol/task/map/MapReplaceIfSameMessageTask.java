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
import com.hazelcast.client.impl.protocol.codec.MapReplaceIfSameCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.Operation;

public class MapReplaceIfSameMessageTask
        extends AbstractMapPutMessageTask<MapReplaceIfSameCodec.RequestParameters> {

    public MapReplaceIfSameMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        MapOperationProvider operationProvider = getMapOperationProvider(parameters.name);
        MapOperation op = operationProvider.createReplaceIfSameOperation(parameters.name, parameters.key,
                parameters.testValue, parameters.value);
        op.setThreadId(parameters.threadId);
        return op;
    }

    @Override
    protected MapReplaceIfSameCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapReplaceIfSameCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapReplaceIfSameCodec.encodeResponse((Boolean) response);
    }


    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "replace";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key, parameters.testValue, parameters.value};
    }
}
