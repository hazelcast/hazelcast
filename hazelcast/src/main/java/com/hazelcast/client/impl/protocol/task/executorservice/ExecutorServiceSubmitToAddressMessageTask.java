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

package com.hazelcast.client.impl.protocol.task.executorservice;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToAddressCodec;
import com.hazelcast.client.impl.protocol.task.InvocationMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.executor.impl.operations.MemberCallableTaskOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import javax.security.auth.Subject;
import java.net.UnknownHostException;
import java.security.Permission;
import java.util.concurrent.Callable;

public class ExecutorServiceSubmitToAddressMessageTask
        extends InvocationMessageTask<ExecutorServiceSubmitToAddressCodec.RequestParameters>
        implements ExecutionCallback {

    public ExecutorServiceSubmitToAddressMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        final InternalOperationService operationService = nodeEngine.getOperationService();
        Address target = null;
        try {
            target = new Address(parameters.hostname, parameters.port);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return operationService.createInvocationBuilder(getServiceName(), op, target);
    }

    @Override
    protected Operation prepareOperation() {
        Callable callable = serializationService.toObject(parameters.callable);
        SecurityContext securityContext = clientEngine.getSecurityContext();
        if (securityContext != null) {
            Subject subject = getEndpoint().getSubject();
            callable = securityContext.createSecureCallable(subject, callable);
        }

        Data callableData = serializationService.toData(callable);

        final MemberCallableTaskOperation op =
                new MemberCallableTaskOperation(parameters.name, parameters.uuid, callableData);
        op.setCallerUuid(endpoint.getUuid());
        return op;
    }

    @Override
    protected ExecutorServiceSubmitToAddressCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ExecutorServiceSubmitToAddressCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        Data data = serializationService.toData(response);
        return ExecutorServiceSubmitToAddressCodec.encodeResponse(data);
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
