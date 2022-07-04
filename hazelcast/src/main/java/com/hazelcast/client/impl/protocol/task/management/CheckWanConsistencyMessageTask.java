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
import com.hazelcast.client.impl.protocol.codec.MCCheckWanConsistencyCodec;
import com.hazelcast.client.impl.protocol.codec.MCCheckWanConsistencyCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ManagementPermission;
import com.hazelcast.wan.impl.WanReplicationService;

import java.security.Permission;
import java.util.UUID;

public class CheckWanConsistencyMessageTask extends AbstractCallableMessageTask<RequestParameters> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("wan.checkConsistency");

    public CheckWanConsistencyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        return nodeEngine.getWanReplicationService().consistencyCheck(
                parameters.wanReplicationName, parameters.wanPublisherId, parameters.mapName);
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCCheckWanConsistencyCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCCheckWanConsistencyCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return WanReplicationService.SERVICE_NAME;
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
        return "checkWanConsistency";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.wanReplicationName, parameters.wanPublisherId, parameters.mapName};
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
