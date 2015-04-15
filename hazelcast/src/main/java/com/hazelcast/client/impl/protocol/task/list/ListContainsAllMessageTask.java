package com.hazelcast.client.impl.protocol.task.list;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.ListContainsAllParameters;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.collection.impl.collection.operations.CollectionContainsOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.spi.Operation;
import java.security.Permission;

/**
 * ListContainsAllMessageTask
 */
public class ListContainsAllMessageTask
        extends AbstractPartitionMessageTask<ListContainsAllParameters> {

    public ListContainsAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new CollectionContainsOperation(parameters.name, parameters.valueSet);
    }

    @Override
    protected ListContainsAllParameters decodeClientMessage(ClientMessage clientMessage) {
        return ListContainsAllParameters.decode(clientMessage);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{ parameters.valueSet};
    }

    @Override
    public Permission getRequiredPermission() {
        return new ListPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getMethodName() {
        return "containsAll";
    }

}
