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
import com.hazelcast.cache.impl.operation.CacheListenerRegistrationOperation;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.CacheListenerRegistrationParameters;
import com.hazelcast.client.impl.protocol.task.InvocationMessageTask;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import java.net.UnknownHostException;
import java.security.Permission;

/**
 * This client request  specifically calls {@link CacheListenerRegistrationOperation} on the server side.
 *
 * @see CacheListenerRegistrationOperation
 */
public class CacheListenerRegistrationMessageTask
        extends InvocationMessageTask<CacheListenerRegistrationParameters> {

    public CacheListenerRegistrationMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        CacheService service = getService(getServiceName());
        CacheEntryListenerConfiguration conf = (CacheEntryListenerConfiguration) service.toObject(parameters.listenerConfig);
        return new CacheListenerRegistrationOperation(parameters.name, conf, parameters.register);
    }

    @Override
    protected CacheListenerRegistrationParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheListenerRegistrationParameters.decode(clientMessage);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        final InternalOperationService operationService = nodeEngine.getOperationService();
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
        return DistributedExecutorService.SERVICE_NAME;
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
