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
import com.hazelcast.client.impl.protocol.codec.MCGetMapConfigCodec;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.config.MapConfigReadOnly;
import com.hazelcast.internal.management.operation.GetMapConfigOperation;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.security.permission.ManagementPermission;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

public class GetMapConfigMessageTask extends AbstractInvocationMessageTask<String> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("map.getConfig");

    public GetMapConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        return nodeEngine.getOperationService().createInvocationBuilder(getServiceName(),
                op, nodeEngine.getThisAddress());
    }

    @Override
    protected Operation prepareOperation() {
        return new GetMapConfigOperation(parameters);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return MCGetMapConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        MapConfigReadOnly config = (MapConfigReadOnly) response;

        EvictionConfig evictionConfig = config.getEvictionConfig();
        int maxSize = evictionConfig.getSize();
        int maxSizePolicyId = evictionConfig.getMaxSizePolicy().getId();
        int evictionPolicyId = evictionConfig.getEvictionPolicy().getId();
        String mergePolicy = config.getMergePolicyConfig().getPolicy();

        return MCGetMapConfigCodec.encodeResponse(
                config.getInMemoryFormat().getId(),
                config.getBackupCount(),
                config.getAsyncBackupCount(),
                config.getTimeToLiveSeconds(),
                config.getMaxIdleSeconds(),
                maxSize,
                maxSizePolicyId,
                config.isReadBackupData(),
                evictionPolicyId,
                mergePolicy,
                config.getIndexConfigs());
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
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
        return "getMapConfig";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters};
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
