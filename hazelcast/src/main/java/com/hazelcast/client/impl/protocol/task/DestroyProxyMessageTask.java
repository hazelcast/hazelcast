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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.DestroyProxyParameters;
import com.hazelcast.client.impl.protocol.parameters.VoidResultParameters;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.ProxyService;

import java.security.Permission;

import static com.hazelcast.security.permission.ActionConstants.getPermission;

public class DestroyProxyMessageTask extends AbstractCallableMessageTask<DestroyProxyParameters> {

    public DestroyProxyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() throws Exception {
        ProxyService proxyService = nodeEngine.getProxyService();
        proxyService.destroyDistributedObject(parameters.serviceName, parameters.name);
        return VoidResultParameters.encode();
    }

    @Override
    protected DestroyProxyParameters decodeClientMessage(ClientMessage clientMessage) {
        return DestroyProxyParameters.decode(clientMessage);
    }

    @Override
    public String getServiceName() {
        return parameters.serviceName;
    }

    @Override
    public Permission getRequiredPermission() {
        return getPermission(parameters.name, parameters.serviceName, ActionConstants.ACTION_DESTROY);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
