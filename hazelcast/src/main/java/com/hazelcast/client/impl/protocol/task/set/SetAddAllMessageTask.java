package com.hazelcast.client.impl.protocol.task.set;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.ListAddAllParameters;
import com.hazelcast.client.impl.protocol.parameters.SetAddAllParameters;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.collection.impl.collection.operations.CollectionAddAllOperation;
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
public class SetAddAllMessageTask
        extends AbstractPartitionMessageTask<SetAddAllParameters> {

    public SetAddAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new CollectionAddAllOperation(parameters.name, parameters.valueList);
    }

    @Override
    protected SetAddAllParameters decodeClientMessage(ClientMessage clientMessage) {
        return SetAddAllParameters.decode(clientMessage);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{ parameters.valueList};
    }

    @Override
    public Permission getRequiredPermission() {
        return new ListPermission(parameters.name, ActionConstants.ACTION_ADD);
    }

    @Override
    public String getMethodName() {
        return "addAll";
    }

}
