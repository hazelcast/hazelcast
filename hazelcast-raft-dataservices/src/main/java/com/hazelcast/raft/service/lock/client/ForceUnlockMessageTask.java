package com.hazelcast.raft.service.lock.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.lock.operation.ForceUnlockOp;

import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class ForceUnlockMessageTask extends AbstractLockMessageTask {

    private long threadId;
    private long expectedFence;
    private UUID invocationUid;

    protected ForceUnlockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftInvocationManager raftInvocationManager = getRaftInvocationManager();
        RaftOp op = new ForceUnlockOp(name, expectedFence, invocationUid);
        ICompletableFuture future = raftInvocationManager.invoke(groupId, op);
        future.andThen(this);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        super.decodeClientMessage(clientMessage);
        threadId = clientMessage.getLong();
        long least = clientMessage.getLong();
        long most = clientMessage.getLong();
        invocationUid = new UUID(most, least);
        expectedFence = clientMessage.getLong();
        return null;
    }
}
