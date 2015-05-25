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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.codec.MapTryPutCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.operation.TryPutOperation;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;

import java.util.concurrent.TimeUnit;

public class MapTryPutMessageTask extends AbstractMapPutMessageTask<MapTryPutCodec.RequestParameters> {

    public MapTryPutMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        TryPutOperation op = new TryPutOperation(parameters.name, parameters.key, parameters.value, parameters.timeout);
        op.setThreadId(parameters.threadId);
        return op;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return BooleanResultParameters.encode((Boolean) response);
    }

    @Override
    protected MapTryPutCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapTryPutCodec.decodeRequest(clientMessage);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "tryPut";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key, parameters.value, parameters.timeout, TimeUnit.MILLISECONDS};
    }

}
