package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCInterruptHotRestartBackupCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;

public class HotRestartBackupInterruptMessageTask extends
                                                  AbstractCallableMessageTask<MCInterruptHotRestartBackupCodec.RequestParameters> {

    private final Node node;

    public HotRestartBackupInterruptMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        this.node = node;
    }

    @Override
    protected Object call()
            throws Exception {
        node.getNodeExtension().getHotRestartService().interruptBackupTask();
        return null;
    }

    @Override
    protected MCInterruptHotRestartBackupCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCInterruptHotRestartBackupCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCInterruptHotRestartBackupCodec.encodeResponse();
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
        return "interruptHotRestartBackup";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
