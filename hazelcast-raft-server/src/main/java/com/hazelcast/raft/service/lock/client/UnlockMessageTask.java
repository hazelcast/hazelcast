package com.hazelcast.raft.service.lock.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.raft.service.lock.proxy.RaftLockProxy;
import com.hazelcast.util.ThreadUtil;

import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 *
 */
public class UnlockMessageTask extends AbstractLockMessageTask {

    private long threadId;
    private UUID invUuid;

    protected UnlockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftLockProxy lockProxy = getProxy();
        ThreadUtil.setThreadId(threadId);
        try {
            lockProxy.unlockAsync(invUuid).andThen(this);
        } finally {
            ThreadUtil.removeThreadId();
        }
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        super.decodeClientMessage(clientMessage);
        threadId = clientMessage.getLong();
        long least = clientMessage.getLong();
        long most = clientMessage.getLong();
        invUuid = new UUID(most, least);
        return null;
    }
}
