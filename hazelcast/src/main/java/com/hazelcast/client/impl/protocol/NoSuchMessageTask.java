package com.hazelcast.client.impl.protocol;

import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

import java.security.Permission;

/**
 *
 */
public class NoSuchMessageTask
        extends AbstractMessageTask<ClientMessage> {

    protected NoSuchMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage decodeClientMessage(ClientMessage clientMessage) {
        return clientMessage;
    }

    @Override
    protected void processMessage() {
        logger.warning("Unrecognized client message received with type:" + this.parameters.getMessageType());

    }

    //Overriding the partition id send from client as it is not recognized
    @Override
    public int getPartitionId() {
        return -1;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
