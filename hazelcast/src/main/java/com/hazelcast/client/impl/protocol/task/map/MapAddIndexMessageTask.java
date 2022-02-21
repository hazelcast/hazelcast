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
import com.hazelcast.client.impl.protocol.codec.MapAddIndexCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAllPartitionsMessageTask;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexType;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.AddIndexOperationFactory;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Map;

public class MapAddIndexMessageTask
        extends AbstractAllPartitionsMessageTask<MapAddIndexCodec.RequestParameters> {

    public MapAddIndexMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new AddIndexOperationFactory(parameters.name, parameters.indexConfig);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        return null;
    }

    @Override
    protected MapAddIndexCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapAddIndexCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapAddIndexCodec.encodeResponse();
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_INDEX);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "addIndex";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] {parameters.indexConfig};
    }

    @Override
    protected void beforeProcess() {
        if (nodeEngine.getConfig().getMapConfig(parameters.name).getInMemoryFormat() == InMemoryFormat.NATIVE
                && parameters.indexConfig.getType() == IndexType.BITMAP) {
            throw new IllegalArgumentException("BITMAP indexes are not supported by NATIVE storage");
        }
    }
}
