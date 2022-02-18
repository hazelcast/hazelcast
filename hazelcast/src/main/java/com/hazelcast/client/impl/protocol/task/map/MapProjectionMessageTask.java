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
import com.hazelcast.client.impl.protocol.codec.MapProjectCodec;
import com.hazelcast.client.impl.protocol.codec.MapProjectWithPredicateCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.projection.Projection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;

import java.security.Permission;
import java.util.List;

public class MapProjectionMessageTask
        extends DefaultMapProjectMessageTask<MapProjectCodec.RequestParameters> {

    public MapProjectionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Projection<?, ?> getProjection() {
        return nodeEngine.getSerializationService().toObject(parameters.projection);
    }

    @Override
    protected MapProjectCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapProjectCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapProjectWithPredicateCodec.encodeResponse((List<Data>) response);
    }

    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_PROJECTION);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "project";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] { parameters.name, parameters.projection };
    }

}
