package com.hazelcast.client.impl.protocol.map;

import com.hazelcast.client.impl.protocol.AbstractPartitionMessageTask;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.operation.PutOperation;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.DefaultData;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

public class MapPutMessageTask
        extends AbstractPartitionMessageTask<MapPutParameters> {

    public MapPutMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    protected Operation prepareOperation() {
        PutOperation op = new PutOperation(parameters.name, new DefaultData(parameters.key), new DefaultData(parameters.value),
                parameters.ttl);
        op.setThreadId(parameters.threadId);
        return op;
    }

    @Override
    protected MapPutParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapPutParameters.decode(clientMessage);
    }

    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_PUT);
    }

    @Override
    public Object[] getParameters() {
        if (parameters.ttl == -1) {
            return new Object[]{parameters.key, parameters.value};
        }
        //TODO what should be the types of the key and value passed to securityContext
        return new Object[]{parameters.key, parameters.value, parameters.ttl, TimeUnit.MILLISECONDS};
    }
}
