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
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToPartitionCodec;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.executor.impl.operations.CallableTaskOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import javax.security.auth.Subject;
import java.security.Permission;
import java.util.concurrent.Callable;

public class ExecutorServiceSubmitToPartitionMessageTask
        extends AbstractInvocationMessageTask<ExecutorServiceSubmitToPartitionCodec.RequestParameters>
        implements ExecutionCallback {

    public ExecutorServiceSubmitToPartitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        if (parameters.partitionId == -1) {
            throw new IllegalArgumentException("Partition id is -1");
        }

        final InternalOperationService operationService = nodeEngine.getOperationService();
        return operationService.createInvocationBuilder(getServiceName(), op, parameters.partitionId);
    }

    @Override
    protected Operation prepareOperation() {
        SecurityContext securityContext = clientEngine.getSecurityContext();
        Data callableData = parameters.callable;
        if (securityContext != null) {
            Subject subject = getEndpoint().getSubject();
            Callable callable = serializationService.toObject(parameters.callable);
            callable = securityContext.createSecureCallable(subject, callable);
            callableData = serializationService.toData(callable);
        }
        return new CallableTaskOperation(parameters.name, parameters.uuid, callableData);
    }


    @Override
    protected ExecutorServiceSubmitToPartitionCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ExecutorServiceSubmitToPartitionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        Data data = serializationService.toData(response);
        return ExecutorServiceSubmitToPartitionCodec.encodeResponse(data);
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
