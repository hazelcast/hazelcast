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
import com.hazelcast.client.impl.protocol.codec.MCApplyMCConfigCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.dto.ClientBwListDTO;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;

public class ApplyMCConfigMessageTask extends AbstractCallableMessageTask<MCApplyMCConfigCodec.RequestParameters> {

    public ApplyMCConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();
        ClientBwListDTO.Mode mode = ClientBwListDTO.Mode.getById(parameters.clientBwListMode);
        if (mode == null) {
            throw new IllegalArgumentException("Unexpected client B/W list mode = [" + parameters.clientBwListMode + "]");
        }
        mcService.applyMCConfig(parameters.eTag, new ClientBwListDTO(mode, parameters.clientBwListEntries));
        return null;
    }

    @Override
    protected MCApplyMCConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCApplyMCConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCApplyMCConfigCodec.encodeResponse();
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
        return "applyMCConfig";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] {
                parameters.eTag,
                parameters.clientBwListMode,
                parameters.clientBwListEntries
        };
    }
}
