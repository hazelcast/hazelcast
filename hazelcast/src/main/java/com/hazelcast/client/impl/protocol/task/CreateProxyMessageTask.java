/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.ClientCreateProxyCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.proxyservice.ProxyService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.proxyservice.impl.operations.InitializeDistributedObjectOperation;

import java.security.Permission;
import java.util.Collection;

public class CreateProxyMessageTask extends AbstractInvocationMessageTask<ClientCreateProxyCodec.RequestParameters>
        implements BlockingMessageTask {

    public CreateProxyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        final OperationServiceImpl operationService = nodeEngine.getOperationService();
        return operationService.createInvocationBuilder(getServiceName(), op, parameters.target).setTryCount(1);
    }

    @Override
    protected Operation prepareOperation() {
        return new InitializeDistributedObjectOperation(parameters.serviceName, parameters.name);
    }

    @Override
    protected ClientCreateProxyCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        parameters = ClientCreateProxyCodec.decodeRequest(clientMessage);
        parameters.target = clientEngine.memberAddressOf(parameters.target);
        return parameters;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientCreateProxyCodec.encodeResponse();
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
