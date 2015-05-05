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
import com.hazelcast.client.impl.protocol.parameters.CreateProxyParameters;
import com.hazelcast.client.impl.protocol.parameters.VoidResultParameters;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.ProxyService;

import java.security.Permission;
import java.util.Collection;

public class CreateProxyMessageTask extends AbstractCallableMessageTask<CreateProxyParameters> {

    public CreateProxyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }


    @Override
    protected ClientMessage call() {
        ProxyService proxyService = clientEngine.getProxyService();
        proxyService.initializeDistributedObject(parameters.serviceName, parameters.name);
        return VoidResultParameters.encode();
    }

    @Override
    protected CreateProxyParameters decodeClientMessage(ClientMessage clientMessage) {
        return CreateProxyParameters.decode(clientMessage);
    }

    @Override
    public String getServiceName() {
        return parameters.serviceName;
    }

    @Override
    public Permission getRequiredPermission() {
        ProxyService proxyService = clientEngine.getProxyService();
        Collection<String> distributedObjectNames = proxyService.getDistributedObjectNames(parameters.serviceName);
        if (distributedObjectNames.contains(parameters.name)) {
            return null;
        }
        return ActionConstants.getPermission(parameters.name, parameters.serviceName, ActionConstants.ACTION_CREATE);
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
