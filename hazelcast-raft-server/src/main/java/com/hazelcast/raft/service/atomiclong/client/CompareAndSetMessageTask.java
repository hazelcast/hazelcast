package com.hazelcast.raft.service.atomiclong.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CompareAndSetMessageTask extends AbstractAtomicLongMessageTask {

    private long expect;
    private long current;

    protected CompareAndSetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() throws Throwable {
        IAtomicLong atomicLong = getProxy();
        ICompletableFuture<Boolean> future = atomicLong.compareAndSetAsync(expect, current);
        future.andThen(this);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        super.decodeClientMessage(clientMessage);
        expect = clientMessage.getLong();
        current = clientMessage.getLong();
        return null;
    }
}
