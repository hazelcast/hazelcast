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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheManagementConfigOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.CacheManagementConfigParameters;
import com.hazelcast.client.impl.protocol.task.InvocationMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.net.UnknownHostException;
import java.security.Permission;

/**
 * This client request  specifically calls {@link CacheManagementConfigOperation} on the server side.
 *
 * @see CacheManagementConfigOperation
 */
public class CacheManagementConfigMessageTask
        extends InvocationMessageTask<CacheManagementConfigParameters> {

    public CacheManagementConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new CacheManagementConfigOperation(parameters.name, parameters.isStat, parameters.enabled);
    }

    @Override
    protected CacheManagementConfigParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheManagementConfigParameters.decode(clientMessage);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        InternalOperationService operationService = nodeEngine.getOperationService();
        Address target = null;
        try {
            target = new Address(parameters.hostname, parameters.port);
        } catch (UnknownHostException e) {
            logger.warning("Cannot parse address : " + parameters.hostname);
        }
        return operationService.createInvocationBuilder(getServiceName(), op, target);
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
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
