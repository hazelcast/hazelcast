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
import com.hazelcast.client.impl.protocol.codec.MCAddWanBatchPublisherConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCAddWanBatchPublisherConfigCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ManagementPermission;
import com.hazelcast.wan.impl.AddWanConfigResult;
import com.hazelcast.wan.impl.WanReplicationService;

import java.security.Permission;

import static com.hazelcast.config.WanBatchPublisherConfig.DEFAULT_ACKNOWLEDGE_TYPE;
import static com.hazelcast.config.WanBatchPublisherConfig.DEFAULT_QUEUE_FULL_BEHAVIOUR;

public class AddWanBatchPublisherConfigMessageTask extends AbstractCallableMessageTask<RequestParameters> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("wan.addBatchPublisherConfig");

    public AddWanBatchPublisherConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        WanReplicationConfig wanConfig = new WanReplicationConfig();
        wanConfig.setName(parameters.name);

        WanBatchPublisherConfig publisherConfig = new WanBatchPublisherConfig();
        publisherConfig.setPublisherId(parameters.publisherId);
        publisherConfig.setClusterName(parameters.targetCluster);
        publisherConfig.setTargetEndpoints(parameters.endpoints);
        publisherConfig.setQueueCapacity(parameters.queueCapacity);
        publisherConfig.setBatchSize(parameters.batchSize);
        publisherConfig.setBatchMaxDelayMillis(parameters.batchMaxDelayMillis);
        publisherConfig.setResponseTimeoutMillis(parameters.responseTimeoutMillis);

        WanAcknowledgeType ackType = WanAcknowledgeType.getById(parameters.ackType);
        publisherConfig.setAcknowledgeType(ackType != null ? ackType : DEFAULT_ACKNOWLEDGE_TYPE);

        WanQueueFullBehavior queueFullBehavior = WanQueueFullBehavior.getByType(parameters.queueFullBehavior);
        publisherConfig.setQueueFullBehavior(
                queueFullBehavior != null ? queueFullBehavior : DEFAULT_QUEUE_FULL_BEHAVIOUR);
        wanConfig.addBatchReplicationPublisherConfig(publisherConfig);

        return nodeEngine.getWanReplicationService().addWanReplicationConfig(wanConfig);
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCAddWanBatchPublisherConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        AddWanConfigResult result = (AddWanConfigResult) response;
        return MCAddWanBatchPublisherConfigCodec.encodeResponse(
                result.getAddedPublisherIds(), result.getIgnoredPublisherIds());
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
        return "addWanBatchPublisherConfig";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{
                parameters.name, parameters.targetCluster, parameters.publisherId, parameters.endpoints,
                parameters.queueCapacity, parameters.batchSize, parameters.batchMaxDelayMillis,
                parameters.responseTimeoutMillis, parameters.ackType, parameters.queueFullBehavior};
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
