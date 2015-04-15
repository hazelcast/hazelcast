package com.hazelcast.client.impl.protocol.task.list;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.ListAddAllWithIndexParameters;
import com.hazelcast.client.impl.protocol.parameters.ListAddParameters;
import com.hazelcast.client.impl.protocol.parameters.ListAddWithIndexParameters;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.collection.impl.collection.operations.CollectionAddOperation;
import com.hazelcast.collection.impl.list.operations.ListAddAllOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;

/**
 * ListAddAllMessageTask
 */
public class ListAddMessageTask
        extends AbstractPartitionMessageTask<ListAddParameters> {

    public ListAddMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new CollectionAddOperation(parameters.name, parameters.value);
    }

    @Override
    protected ListAddParameters decodeClientMessage(ClientMessage clientMessage) {
        return ListAddParameters.decode(clientMessage);
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
        return "add";
    }

}
