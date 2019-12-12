package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCTriggerPartialStartCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;

public class TriggerPartialRestartMessageTask extends AbstractCallableMessageTask<MCTriggerPartialStartCodec.RequestParameters> {

    private final Node node;

    public TriggerPartialRestartMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        this.node = node;
    }

    @Override
    protected Object call()
            throws Exception {
        return node.getNodeExtension().getInternalHotRestartService().triggerPartialStart();
    }

    @Override
    protected MCTriggerPartialStartCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCTriggerPartialStartCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCTriggerPartialStartCodec.encodeResponse((Boolean) response);
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
        return "triggerPartialRestart";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
