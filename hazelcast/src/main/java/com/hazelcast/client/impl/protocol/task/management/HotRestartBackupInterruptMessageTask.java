package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCInterruptHotRestartBackupCodec;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.operation.AbstractManagementOperation;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

import static com.hazelcast.internal.management.ManagementDataSerializerHook.HOT_RESTART_BACKUP_INTERRUPT_OPERATION;

class HotRestartBackupInterruptOperation extends AbstractManagementOperation {

    private final Node node;

    public HotRestartBackupInterruptOperation(Node node) {
        this.node = node;
    }

    @Override
    public void run()
            throws Exception {
        node.getNodeExtension().getHotRestartService().interruptBackupTask();
    }

    @Override
    public int getClassId() {
        return HOT_RESTART_BACKUP_INTERRUPT_OPERATION;
    }
}

public class HotRestartBackupInterruptMessageTask extends AbstractInvocationMessageTask<MCInterruptHotRestartBackupCodec.RequestParameters> {

    private final Node node;

    public HotRestartBackupInterruptMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        this.node = node;
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        return nodeEngine.getOperationService().createInvocationBuilder(getServiceName(),
                op, nodeEngine.getThisAddress());
    }

    @Override
    protected Operation prepareOperation() {
        return new HotRestartBackupInterruptOperation(node);
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
