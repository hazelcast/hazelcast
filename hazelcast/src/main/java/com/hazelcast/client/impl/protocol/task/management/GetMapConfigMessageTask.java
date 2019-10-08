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
import com.hazelcast.client.impl.protocol.codec.MCGetMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetMapConfigCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.config.MapConfigReadOnly;
import com.hazelcast.internal.management.operation.GetMapConfigOperation;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

import static com.hazelcast.config.MapConfig.DEFAULT_EVICTION_POLICY;
import static com.hazelcast.config.MaxSizeConfig.DEFAULT_MAX_SIZE;
import static com.hazelcast.config.MergePolicyConfig.DEFAULT_MERGE_POLICY;

public class GetMapConfigMessageTask extends AbstractInvocationMessageTask<RequestParameters> {
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
        return new GetMapConfigOperation(parameters.mapName);
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCGetMapConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        MapConfigReadOnly config = (MapConfigReadOnly) response;

        int maxSize = config.getMaxSizeConfig() != null
                ? config.getMaxSizeConfig().getSize()
                : DEFAULT_MAX_SIZE;
        int maxSizePolicy = config.getMaxSizeConfig() != null
                ? config.getMaxSizeConfig().getMaxSizePolicy().getId()
                : MaxSizeConfig.MaxSizePolicy.PER_NODE.getId();
        int evictionPolicy = config.getEvictionPolicy() != null
                ? config.getEvictionPolicy().getId()
                : DEFAULT_EVICTION_POLICY.getId();
        String mergePolicy = config.getMergePolicyConfig() != null
                ? config.getMergePolicyConfig().getPolicy()
                : DEFAULT_MERGE_POLICY;

        return MCGetMapConfigCodec.encodeResponse(
                config.getInMemoryFormat().getId(),
                config.getBackupCount(),
                config.getAsyncBackupCount(),
                config.getTimeToLiveSeconds(),
                config.getMaxIdleSeconds(),
                maxSize,
                maxSizePolicy,
                config.isReadBackupData(),
                evictionPolicy,
                mergePolicy);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
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
        return null;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.mapName};
    }
}
