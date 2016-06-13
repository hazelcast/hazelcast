/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.jet;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.permission.JetPermission;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.impl.hazelcast.JetService;
import com.hazelcast.jet.impl.operation.JetOperation;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.security.Permission;

public abstract class JetMessageTask<P> extends AbstractMessageTask<P> implements ExecutionCallback<Object> {
    protected JetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public String getServiceName() {
        return JetService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new JetPermission(getApplicationName(), ActionConstants.ACTION_ALL);
    }


    @Override
    public String getDistributedObjectName() {
        return getApplicationName();
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    protected void processMessage() throws Throwable {
        InternalOperationService os = nodeEngine.getOperationService();
        JetOperation operation = prepareOperation();
        operation.setCallerUuid(getEndpoint().getUuid());

        InvocationBuilder invocation = os.createInvocationBuilder(getServiceName(),
                operation, nodeEngine.getThisAddress())
                .setExecutionCallback(this);

        invocation.invoke();
    }

    protected abstract String getApplicationName();

    protected abstract JetOperation prepareOperation();

    @Override
    public void onResponse(Object response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        handleProcessingFailure(t);
    }
}
