package com.hazelcast.raft.service.atomiclong.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

/**
 * TODO: Javadoc Pending...
 *
 */
public class GetAndAddMessageTask extends AbstractAtomicLongMessageTask {

    private long delta;

    protected GetAndAddMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        IAtomicLong atomicLong = getProxy();
        atomicLong.getAndAddAsync(delta).andThen(this);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        super.decodeClientMessage(clientMessage);
        delta = clientMessage.getLong();
        return null;
    }
}
