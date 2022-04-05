/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.executorservice.durable;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorSubmitToPartitionCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.durableexecutor.impl.operations.TaskOperation;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.security.auth.Subject;
import java.security.Permission;
import java.util.concurrent.Callable;

import static com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService.SERVICE_NAME;

public class DurableExecutorSubmitToPartitionMessageTask
        extends AbstractPartitionMessageTask<DurableExecutorSubmitToPartitionCodec.RequestParameters> {

    public DurableExecutorSubmitToPartitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
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
        return new TaskOperation(parameters.name, callableData);
    }


    @Override
    protected DurableExecutorSubmitToPartitionCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DurableExecutorSubmitToPartitionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DurableExecutorSubmitToPartitionCodec.encodeResponse((Integer) response);
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
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
