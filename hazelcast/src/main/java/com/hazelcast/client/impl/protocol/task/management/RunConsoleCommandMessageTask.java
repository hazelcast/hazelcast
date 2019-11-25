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

package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCRunConsoleCommandCodec;
import com.hazelcast.client.impl.protocol.codec.MCRunConsoleCommandCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.client.impl.protocol.task.BlockingMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.ConsoleCommandHandler;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

public class RunConsoleCommandMessageTask
        extends AbstractCallableMessageTask<RequestParameters> implements BlockingMessageTask {

    public RunConsoleCommandMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();
        ConsoleCommandHandler handler = mcService.getCommandHandler();
        try {
            final String ns = parameters.namespace;
            String command = parameters.command;
            if (!isNullOrEmpty(ns)) {
                // set namespace as a part of the command
                command = ns + "__" + command;
            }
            return handler.handleCommand(command);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCRunConsoleCommandCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCRunConsoleCommandCodec.encodeResponse((String) response);
    }

    @Override
    public String getServiceName() {
        return ManagementCenterService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "runConsoleCommand";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] {
                parameters.command,
                parameters.namespace
        };
    }
}
