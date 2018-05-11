package com.hazelcast.raft.service.atomiclong.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Connection;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;

import java.security.Permission;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CreateAtomicLongMessageTask extends AbstractMessageTask implements ExecutionCallback {

    private String name;

    protected CreateAtomicLongMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftAtomicLongService service = nodeEngine.getService(RaftAtomicLongService.SERVICE_NAME);
        service.createRaftGroup(name).andThen(this);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        name = clientMessage.getStringUtf8();
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
