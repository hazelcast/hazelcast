package com.hazelcast.client.impl.protocol.task.list;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.ListAddParameters;
import com.hazelcast.client.impl.protocol.parameters.ListContainsParameters;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.collection.impl.collection.operations.CollectionAddOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;

/**
 * ListAddAllMessageTask
 */
public class ListContainsMessageTask
        extends AbstractPartitionMessageTask<ListContainsParameters> {

    public ListContainsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new CollectionAddOperation(parameters.name, parameters.value);
    }

    @Override
    protected ListContainsParameters decodeClientMessage(ClientMessage clientMessage) {
        return ListContainsParameters.decode(clientMessage);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{ parameters.value};
    }

    @Override
    public Permission getRequiredPermission() {
        return new ListPermission(parameters.name, ActionConstants.ACTION_ADD);
    }

    @Override
    public String getMethodName() {
        return "contains";
    }

}
