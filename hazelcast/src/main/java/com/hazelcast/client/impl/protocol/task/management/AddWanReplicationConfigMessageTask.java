package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCAddWanReplicationConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCAddWanReplicationConfigCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.wan.impl.AddWanConfigResult;
import com.hazelcast.wan.impl.WanReplicationService;

import java.security.Permission;

import static com.hazelcast.config.WanBatchReplicationPublisherConfig.DEFAULT_ACKNOWLEDGE_TYPE;
import static com.hazelcast.config.WanBatchReplicationPublisherConfig.DEFAULT_QUEUE_FULL_BEHAVIOUR;

public class AddWanReplicationConfigMessageTask extends AbstractCallableMessageTask<RequestParameters> {
    public AddWanReplicationConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        WanReplicationConfig wanConfig = new WanReplicationConfig();
        wanConfig.setName(parameters.name);

        WanBatchReplicationPublisherConfig publisherConfig = new WanBatchReplicationPublisherConfig();
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
        wanConfig.addWanBatchReplicationPublisherConfig(publisherConfig);

        return nodeEngine.getWanReplicationService().addWanReplicationConfig(wanConfig);
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCAddWanReplicationConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        AddWanConfigResult result = (AddWanConfigResult) response;
        return MCAddWanReplicationConfigCodec.encodeResponse(
                result.getAddedPublisherIds(), result.getIgnoredPublisherIds());
    }

    @Override
    public String getServiceName() {
        return WanReplicationService.SERVICE_NAME;
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
        return "addWanReplicationConfig";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{
                parameters.name, parameters.targetCluster, parameters.publisherId, parameters.endpoints,
                parameters.queueCapacity, parameters.batchSize, parameters.batchMaxDelayMillis,
                parameters.responseTimeoutMillis, parameters.ackType, parameters.queueFullBehavior};
    }
}
