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
import com.hazelcast.client.impl.protocol.codec.MCApplyMCConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCApplyMCConfigCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.dto.ClientBwListDTO;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ManagementPermission;

import java.security.Permission;

public class ApplyClientFilteringConfigMessageTask extends AbstractCallableMessageTask<RequestParameters> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("clientfiltering.applyConfig");

    public ApplyClientFilteringConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        ManagementCenterService mcs = nodeEngine.getManagementCenterService();
        if (mcs == null) {
            throw new HazelcastException("ManagementCenterService is not initialized yet");
        }
        ClientBwListDTO.Mode mode = ClientBwListDTO.Mode.getById(parameters.clientBwListMode);
        if (mode == null) {
            throw new IllegalArgumentException("Unexpected client B/W list mode = [" + parameters.clientBwListMode + "]");
        }
        mcs.applyMCConfig(parameters.eTag, new ClientBwListDTO(mode, parameters.clientBwListEntries));
        return null;
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
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
        return REQUIRED_PERMISSION;
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

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
