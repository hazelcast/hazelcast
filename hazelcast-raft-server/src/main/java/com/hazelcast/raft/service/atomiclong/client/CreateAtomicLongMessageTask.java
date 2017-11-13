package com.hazelcast.raft.service.atomiclong.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Connection;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;

import java.security.Permission;

import static com.hazelcast.raft.service.atomiclong.RaftAtomicLongService.PREFIX;
import static com.hazelcast.raft.service.atomiclong.RaftAtomicLongService.SERVICE_NAME;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CreateAtomicLongMessageTask extends AbstractMessageTask implements ExecutionCallback {

    private String name;
    private int nodeCount;

    protected CreateAtomicLongMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() throws Throwable {
        String raftName = PREFIX + name;

        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        ICompletableFuture future = invocationManager.createRaftGroupAsync(SERVICE_NAME, raftName, nodeCount);
        future.andThen(this);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        name = clientMessage.getStringUtf8();
        nodeCount = clientMessage.getInt();
        return null;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        if (response instanceof RaftGroupId) {
            RaftGroupId groupId = (RaftGroupId) response;
            int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId) + Bits.LONG_SIZE_IN_BYTES;
            ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
            clientMessage.setMessageType(1111);
            RaftGroupIdImpl.writeTo(groupId, clientMessage);
            clientMessage.updateFrameLength();
            return clientMessage;
        }
        throw new IllegalArgumentException("Unknown response: " + response);
    }

    @Override
    public void onResponse(Object response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        handleProcessingFailure(t);
    }

    @Override
    public String getServiceName() {
        return RaftAtomicLongService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public Permission getRequiredPermission() {
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
