package com.hazelcast.client.impl.protocol.task.queue;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.QueueResetAgeStatisticsCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;

public class QueueResetAgeStatisticsMessageTask extends AbstractCallableMessageTask<String> {

    public QueueResetAgeStatisticsMessageTask(ClientMessage clientMessage, Node node,
                                              Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call()
            throws Exception {
        QueueService service = getService(getServiceName());
        service.resetAgeStats(parameters);
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return QueueResetAgeStatisticsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return QueueResetAgeStatisticsCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
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
