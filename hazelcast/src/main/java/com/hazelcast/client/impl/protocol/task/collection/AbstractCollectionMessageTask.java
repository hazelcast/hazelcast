package com.hazelcast.client.impl.protocol.task.collection;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.security.permission.SetPermission;

import java.security.Permission;

/**
 * Base Collection task
 */
public abstract class AbstractCollectionMessageTask<P>
        extends AbstractPartitionMessageTask<P> {

    protected AbstractCollectionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

//    @Override
//    public Permission getRequiredPermission() {
//        final String action = getRequiredAction();
//        if (ListService.SERVICE_NAME.equals(getServiceName())) {
//            return new ListPermission(getName(), action);
//        } else if (SetService.SERVICE_NAME.equals(getServiceName())) {
//            return new SetPermission(getName(), action);
//        }
//        throw new IllegalArgumentException("No service matched!!!");
//    }

    protected abstract String getName();

    protected abstract String getRequiredAction();
}
