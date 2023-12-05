/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapForceUnlockCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.internal.locksupport.LockSupportService;
import com.hazelcast.internal.locksupport.operations.UnlockOperation;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

public class MapForceUnlockMessageTask
        extends AbstractPartitionMessageTask<MapForceUnlockCodec.RequestParameters> {

    public MapForceUnlockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new UnlockOperation(getObjectNamespace(), parameters.key, -1, true, parameters.referenceId);
    }

    @Override
    protected MapForceUnlockCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapForceUnlockCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapForceUnlockCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return LockSupportService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectType() {
        return MapService.SERVICE_NAME;
    }


    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_LOCK);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    private ObjectNamespace getObjectNamespace() {
        return MapService.getObjectNamespace(parameters.name);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.FORCE_UNLOCK;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key};
    }

    @Override
    protected String getNamespace() {
        // This task is not Namespace-aware so it doesn't matter
        return null;
    }
}

