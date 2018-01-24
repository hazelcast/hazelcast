package com.hazelcast.raft.service.lock.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Connection;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.service.lock.RaftLockService;
import com.hazelcast.raft.service.lock.proxy.RaftLockProxy;

import java.security.Permission;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class AbstractLockMessageTask extends AbstractMessageTask implements ExecutionCallback {

    protected RaftGroupId groupId;
    protected String name;
    protected String uid;

    protected AbstractLockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    protected RaftLockProxy getProxy() {
        RaftLockService service = (RaftLockService) getService(getServiceName());
        // TODO: creates a new proxy on each client request
        return service.newProxy(name, groupId, uid);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        groupId = RaftGroupIdImpl.readFrom(clientMessage);
        name = clientMessage.getStringUtf8();
        uid = clientMessage.getStringUtf8();
        return null;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        if (response instanceof Integer) {
            return encodeLongResponse((Integer) response);
        }
        if (response instanceof Boolean) {
            return encodeBooleanResponse((Boolean) response);
        }
        throw new IllegalArgumentException("Unknown response: " + response);
    }

    private ClientMessage encodeLongResponse(long response) {
        int dataSize = ClientMessage.HEADER_SIZE + Bits.LONG_SIZE_IN_BYTES;
        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.setMessageType(1111);
        clientMessage.set(response);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    private ClientMessage encodeBooleanResponse(boolean response) {
        int dataSize = ClientMessage.HEADER_SIZE + Bits.BOOLEAN_SIZE_IN_BYTES;
        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.setMessageType(1111);
        clientMessage.set(response);
        clientMessage.updateFrameLength();
        return clientMessage;
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
        return RaftLockService.SERVICE_NAME;
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
