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

package com.hazelcast.client.impl.protocol.task.executorservice;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToAddressCodec;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.executor.impl.operations.MemberCallableTaskOperation;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import javax.security.auth.Subject;
import java.security.Permission;
import java.util.concurrent.Callable;

public class ExecutorServiceSubmitToAddressMessageTask
        extends AbstractInvocationMessageTask<ExecutorServiceSubmitToAddressCodec.RequestParameters> {

    public ExecutorServiceSubmitToAddressMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        final OperationServiceImpl operationService = nodeEngine.getOperationService();
        return operationService.createInvocationBuilder(getServiceName(), op, parameters.address);
    }

    @Override
    protected Operation prepareOperation() {
        SecurityContext securityContext = clientEngine.getSecurityContext();
        Data callableData = parameters.callable;
        if (securityContext != null) {
            Subject subject = endpoint.getSubject();
            Object taskObject = serializationService.toObject(parameters.callable);
            Callable callable;
            if (taskObject instanceof Runnable) {
                callable = securityContext.createSecureCallable(subject, (Runnable) taskObject);
            } else {
                callable = securityContext.createSecureCallable(subject, (Callable<? extends Object>) taskObject);
            }
            callableData = serializationService.toData(callable);
        }

        MemberCallableTaskOperation op = new MemberCallableTaskOperation(parameters.name, parameters.uuid, callableData);
        op.setCallerUuid(endpoint.getUuid());
        return op;
    }

    @Override
    protected ExecutorServiceSubmitToAddressCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        parameters = ExecutorServiceSubmitToAddressCodec.decodeRequest(clientMessage);
        parameters.address = clientEngine.memberAddressOf(parameters.address);
        return parameters;
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
