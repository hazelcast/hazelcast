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

package com.hazelcast.client.impl.protocol.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.parameters.MapRemoveInterceptorParameters;
import com.hazelcast.client.impl.protocol.task.AbstractMultiTargetMessageTask;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.RemoveInterceptorOperationFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;

import java.security.Permission;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class MapRemoveInterceptorMessageTask extends AbstractMultiTargetMessageTask<MapRemoveInterceptorParameters> {

    public MapRemoveInterceptorMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new RemoveInterceptorOperationFactory(parameters.id, parameters.name);
    }

    @Override
    protected ClientMessage reduce(Map<Address, Object> map) throws Throwable {
        for (Object result : map.values()) {
            if (result instanceof Throwable) {
                throw (Throwable) result;
            }
        }
        return BooleanResultParameters.encode(true);
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
    protected MapRemoveInterceptorParameters decodeClientMessage(ClientMessage clientMessage) {
        return null;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_INTERCEPT);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "removeInterceptor";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.id};
    }
}
