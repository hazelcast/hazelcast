/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.MapReplaceAllCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.ReplaceAllOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.function.BiFunction;

public class MapReplaceAllMessageTask
        extends AbstractPartitionMessageTask<MapReplaceAllCodec.RequestParameters> {

    private BiFunction function;
    private String name;

    public MapReplaceAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new ReplaceAllOperation(name, function);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    protected MapReplaceAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        MapReplaceAllCodec.RequestParameters parameters =  MapReplaceAllCodec.decodeRequest(clientMessage);
        function = serializationService.toObject(parameters.function);
        name = parameters.name;
        return parameters;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapReplaceAllCodec.encodeResponse();
    } //we had get all response method

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "replaceAll";
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters};
    }
}
