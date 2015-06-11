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
import com.hazelcast.client.impl.protocol.codec.MapAddInterceptorCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMultiTargetMessageTask;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.AddInterceptorOperationFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;

import java.security.Permission;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class MapAddInterceptorMessageTask
        extends AbstractMultiTargetMessageTask<MapAddInterceptorCodec.RequestParameters> {

    private transient String id;

    public MapAddInterceptorMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        final MapService mapService = getService(MapService.SERVICE_NAME);
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final MapInterceptor mapInterceptor = serializationService.toObject(parameters.interceptor);
        id = mapServiceContext.generateInterceptorId(parameters.name, mapInterceptor);
        return new AddInterceptorOperationFactory(id, parameters.name, mapInterceptor);
    }

    @Override
    protected Object reduce(Map<Address, Object> map) throws Throwable {
        for (Object result : map.values()) {
            if (result instanceof Throwable) {
                throw (Throwable) result;
            }
        }
        return id;
    }


    @Override
    public Collection<Address> getTargets() {
        Collection<MemberImpl> memberList = nodeEngine.getClusterService().getMemberList();
        Collection<Address> addresses = new HashSet<Address>();
        for (MemberImpl member : memberList) {
            addresses.add(member.getAddress());
        }
        return addresses;
    }

    @Override
    protected MapAddInterceptorCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapAddInterceptorCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapAddInterceptorCodec.encodeResponse((String) response);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_INTERCEPT);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "addInterceptor";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.interceptor};
    }
}
