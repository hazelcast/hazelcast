package com.hazelcast.raft.service.atomiclong.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.atomiclong.operation.GetAndAddOp;

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
        RaftInvocationManager raftInvocationManager = getRaftInvocationManager();
        ICompletableFuture<Object> future = raftInvocationManager.invoke(groupId, new GetAndAddOp(name, delta));
        future.andThen(this);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        super.decodeClientMessage(clientMessage);
        delta = clientMessage.getLong();
        return null;
    }
}
