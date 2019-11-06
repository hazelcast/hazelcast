package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCGetTimedMemberStateCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetTimedMemberStateCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;

public class GetTimedMemberStateMessageTask extends AbstractCallableMessageTask<RequestParameters> {

    public GetTimedMemberStateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        ManagementCenterService mcs = nodeEngine.getManagementCenterService();
        return mcs != null ? mcs.getTimedMemberStateJson().orElse(null) : null;
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCGetTimedMemberStateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCGetTimedMemberStateCodec.encodeResponse((String) response);
    }

    @Override
    public String getServiceName() {
        return ManagementCenterService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "getTimedMemberState";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
