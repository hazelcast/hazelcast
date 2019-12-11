package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCForceStartCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;

public class ForceStartClusterMessageTask extends AbstractCallableMessageTask<MCForceStartCodec.RequestParameters> {

    private Node node;

    public ForceStartClusterMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        this.node = node;
    }

    @Override
    protected Object call()
            throws Exception {
        return node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
    }

    @Override
    protected MCForceStartCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCForceStartCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCForceStartCodec.encodeResponse((Boolean) response);
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
        return "forceStart";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
