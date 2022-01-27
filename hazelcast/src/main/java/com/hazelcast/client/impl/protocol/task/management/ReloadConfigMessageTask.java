package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

public class ReloadConfigMessageTask
        extends AbstractInvocationMessageTask<Void> {
    protected ReloadConfigMessageTask(ClientMessage clientMessage, Node node,
                                      Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        return null;
    }

    @Override
    protected Operation prepareOperation() {
        return null;
    }

    @Override
    protected Void decodeClientMessage(ClientMessage clientMessage) {
        return null;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return null;
    }

    @Override
    public String getServiceName() {
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
        return new Object[0];
    }
}
