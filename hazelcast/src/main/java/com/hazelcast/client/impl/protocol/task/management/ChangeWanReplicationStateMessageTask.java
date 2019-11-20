package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCChangeWanReplicationStateCodec;
import com.hazelcast.client.impl.protocol.codec.MCChangeWanReplicationStateCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.operation.ChangeWanStateOperation;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.wan.impl.WanReplicationService;

import java.security.Permission;

public class ChangeWanReplicationStateMessageTask extends AbstractInvocationMessageTask<RequestParameters> {
    public ChangeWanReplicationStateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        return nodeEngine.getOperationService().createInvocationBuilder(getServiceName(),
                op, nodeEngine.getThisAddress());
    }

    @Override
    protected Operation prepareOperation() {
        return new ChangeWanStateOperation(
                parameters.wanReplicationName,
                parameters.wanPublisherId,
                WanPublisherState.getByType(parameters.newState));
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCChangeWanReplicationStateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCChangeWanReplicationStateCodec.encodeResponse();
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
        return "changeWanReplicationState";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] {parameters.wanReplicationName, parameters.wanPublisherId, parameters.newState};
    }
}
