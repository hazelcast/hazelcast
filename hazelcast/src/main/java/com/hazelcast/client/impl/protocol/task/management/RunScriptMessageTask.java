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

package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCRunScriptCodec;
import com.hazelcast.client.impl.protocol.codec.MCRunScriptCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.operation.RunScriptOperation;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ManagementPermission;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

public class RunScriptMessageTask extends AbstractInvocationMessageTask<RequestParameters> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("member.runScript");

    public RunScriptMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        return nodeEngine.getOperationService().createInvocationBuilder(getServiceName(), op, nodeEngine.getThisAddress());
    }

    @Override
    protected Operation prepareOperation() {
        return new RunScriptOperation(parameters.engine, parameters.script);
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCRunScriptCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCRunScriptCodec.encodeResponse((String) response);
    }

    @Override
    public String getServiceName() {
        return ManagementCenterService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return REQUIRED_PERMISSION;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "runScript";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] {
                parameters.script,
                parameters.engine
        };
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
