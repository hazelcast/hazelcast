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

import java.security.Permission;

import com.hazelcast.instance.Node;
import com.hazelcast.jet.impl.application.localization.Chunk;
import com.hazelcast.nio.Connection;
import com.hazelcast.jet.api.hazelcast.JetService;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.client.impl.protocol.codec.JetLocalizeCodec;
import com.hazelcast.client.impl.protocol.permission.JetPermission;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.jet.impl.operation.application.LocalizationChunkOperation;

public class JetLocalizeMessageTask extends AbstractMessageTask<JetLocalizeCodec.RequestParameters> {
    public JetLocalizeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected JetLocalizeCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return JetLocalizeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return JetLocalizeCodec.encodeResponse(true);
    }

    @Override
    protected void processMessage() {
        Chunk chunk = this.serializationService.toObject(this.parameters.chunk);

        try {
            new LocalizationChunkOperation(this.parameters.name, chunk, this.nodeEngine).run();
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
        return "localize";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
