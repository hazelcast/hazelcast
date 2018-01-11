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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapFlushCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ProxyService;

import java.security.Permission;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

public class MapFlushMessageTask
        extends AbstractCallableMessageTask<MapFlushCodec.RequestParameters> {

    public MapFlushMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        MapService mapService = getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        ProxyService proxyService = nodeEngine.getProxyService();
        DistributedObject distributedObject = proxyService.getDistributedObject(SERVICE_NAME, parameters.name);

        MapProxyImpl mapProxy = (MapProxyImpl) distributedObject;
        mapProxy.flush();

        return null;
    }


    @Override
    protected MapFlushCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapFlushCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapFlushCodec.encodeResponse();
    }


    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "flush";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

}
