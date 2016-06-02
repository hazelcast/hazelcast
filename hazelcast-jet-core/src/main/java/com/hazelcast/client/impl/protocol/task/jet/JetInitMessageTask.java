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
import com.hazelcast.client.impl.protocol.codec.JetInitCodec;
import com.hazelcast.client.impl.protocol.permission.JetPermission;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.internal.api.hazelcast.JetService;
import com.hazelcast.jet.internal.impl.operation.application.InitApplicationRequestOperation;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;

import java.security.Permission;

public class JetInitMessageTask extends AbstractMessageTask<JetInitCodec.RequestParameters> {
    public JetInitMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected JetInitCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return JetInitCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return JetInitCodec.encodeResponse(true);
    }

    @Override
    protected void processMessage() {
        try {
            JetApplicationConfig config = this.nodeEngine.getSerializationService().toObject(this.parameters.config);
            new InitApplicationRequestOperation(getDistributedObjectName(), this.nodeEngine, config).run();
            sendResponse(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getServiceName() {
        return JetService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new JetPermission(this.parameters.name, ActionConstants.ACTION_ALL);
    }

    @Override
    public String getDistributedObjectName() {
        return this.parameters.name;
    }

    @Override
    public String getMethodName() {
        return "init";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
