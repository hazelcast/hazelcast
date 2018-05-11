package com.hazelcast.raft.service.atomiclong.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.atomiclong.operation.CompareAndSetOp;

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
    protected void processMessage() {
        RaftInvocationManager raftInvocationManager = getRaftInvocationManager();
        ICompletableFuture<Object> future = raftInvocationManager.invoke(groupId, new CompareAndSetOp(name, expect, current));
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
