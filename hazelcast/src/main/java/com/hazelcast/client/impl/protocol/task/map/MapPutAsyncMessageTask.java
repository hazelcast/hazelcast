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
import com.hazelcast.client.impl.protocol.codec.MapPutAsyncCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.operation.PutOperation;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;

import java.util.concurrent.TimeUnit;

public class MapPutAsyncMessageTask extends AbstractMapPutMessageTask<MapPutAsyncCodec.RequestParameters> {

    public MapPutAsyncMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        PutOperation op = new PutOperation(parameters.name, parameters.key, parameters.value, parameters.ttl);
        op.setThreadId(parameters.threadId);
        return op;
    }

    @Override
    protected MapPutAsyncCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapPutAsyncCodec.decodeRequest(clientMessage);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        if (parameters.ttl == -1) {
            return new Object[]{parameters.key, parameters.value};
        }
        return new Object[]{parameters.key, parameters.value, parameters.ttl, TimeUnit.MILLISECONDS};
    }

    @Override
    public String getMethodName() {
        return "putAsync";
    }
}
