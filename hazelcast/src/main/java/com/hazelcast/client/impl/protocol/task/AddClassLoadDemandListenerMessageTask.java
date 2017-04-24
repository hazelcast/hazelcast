package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.ClientClassLoadDemandService;
import com.hazelcast.client.impl.ClientClassLoadDemandService.ClassLoadDemandEvent;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddClassLoadDemandListenerCodec;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.impl.eventservice.InternalEventService;

import java.security.Permission;
import java.util.EventListener;


public class AddClassLoadDemandListenerMessageTask
        extends AbstractCallableMessageTask<ClientAddClassLoadDemandListenerCodec.RequestParameters> implements EventListener {


    public AddClassLoadDemandListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    protected Object call() throws Exception {
        InternalEventService eventService = nodeEngine.getEventService();
        EventRegistration eventRegistration;
        String serviceName = getServiceName();
        ClassLoadDemandListener listener = new ClassLoadDemandListener();
        if (parameters.localOnly) {
            eventRegistration = eventService.registerLocalListener(serviceName, serviceName, listener);
        } else {
            eventRegistration = eventService.registerListener(serviceName, serviceName, listener);
        }
        String eventRegistrationId = eventRegistration.getId();
        endpoint.addListenerDestroyAction(MapService.SERVICE_NAME, serviceName, eventRegistrationId);
        return eventRegistrationId;
    }

    @Override
    protected ClientAddClassLoadDemandListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientAddClassLoadDemandListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientAddClassLoadDemandListenerCodec.encodeResponse((String) response);
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
    public Object[] getParameters() {
        return new Object[0];
    }

    public class ClassLoadDemandListener {

        public void handle(ClassLoadDemandEvent event) {
            if (!endpoint.isAlive()) {
                return;
            }
            String className = event.getDemandedClassName();
            Member member = event.getDemandingMember();
            ClientMessage message = ClientAddClassLoadDemandListenerCodec.encodeClassLoadDemandEvent(className, member);
            sendClientMessage(null, message);
        }
    }

}
