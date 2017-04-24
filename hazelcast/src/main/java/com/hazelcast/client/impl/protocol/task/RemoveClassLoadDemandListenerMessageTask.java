package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.ClientClassLoadDemandService;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientRemoveClassLoadDemandListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientRemoveClassLoadDemandListenerCodec.RequestParameters;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.eventservice.InternalEventService;

import java.security.Permission;

public class RemoveClassLoadDemandListenerMessageTask
        extends AbstractRemoveListenerMessageTask<ClientRemoveClassLoadDemandListenerCodec.RequestParameters> {

    public RemoveClassLoadDemandListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientRemoveClassLoadDemandListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientRemoveClassLoadDemandListenerCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return ClientClassLoadDemandService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    protected boolean deRegisterListener() {
        InternalEventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(getServiceName(), getServiceName(), parameters.registrationId);
    }

    @Override
    protected String getRegistrationId() {
        return parameters.registrationId;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
