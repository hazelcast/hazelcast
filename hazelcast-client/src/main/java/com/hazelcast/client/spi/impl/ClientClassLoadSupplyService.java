package com.hazelcast.client.spi.impl;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddClassLoadDemandListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientRemoveClassLoadDemandListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientSupplyClassLoadCodec;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.core.Member;
import com.hazelcast.internal.usercodedeployment.impl.ClassDataProvider;
import com.hazelcast.internal.usercodedeployment.impl.ClassSource;

import java.util.concurrent.ConcurrentHashMap;

public class ClientClassLoadSupplyService {

    private final HazelcastClientInstanceImpl clientInstance;
    private final ClassDataProvider classDataProvider;
    private final boolean isEnabled;

    public ClientClassLoadSupplyService(HazelcastClientInstanceImpl clientInstance) {
        this.clientInstance = clientInstance;
        this.isEnabled = clientInstance.getClientConfig().isUserCodeDeploymentEnabled();
        this.classDataProvider = new ClassDataProvider(UserCodeDeploymentConfig.ProviderMode.LOCAL_CLASSES_ONLY,
                clientInstance.getConfigClassloader(clientInstance.getClientConfig()),
                new ConcurrentHashMap<String, ClassSource>(),
                clientInstance.getLoggingService().getLogger(ClientClassLoadSupplyService.class));
    }

    public void start() {
        if (isEnabled) {
            ClientListenerService listenerService = clientInstance.getListenerService();
            listenerService.registerListener(createClassLoadDemandCodec(), createClassLoadDemandHandler());
        }
    }

    private ListenerMessageCodec createClassLoadDemandCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ClientAddClassLoadDemandListenerCodec.encodeRequest(localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return ClientAddClassLoadDemandListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String registrationId) {
                return ClientRemoveClassLoadDemandListenerCodec.encodeRequest(registrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ClientRemoveClassLoadDemandListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }


    private EventHandler createClassLoadDemandHandler() {
        return new ClientAddDemandListener();
    }

    private class ClientAddDemandListener
            extends ClientAddClassLoadDemandListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {


        @Override
        public void beforeListenerRegister() {

        }

        @Override
        public void onListenerRegister() {

        }

        @Override
        public void handle(String className, Member member) {
            byte[] classDefinition = classDataProvider.getClassDataOrNull(className).getClassDefinition();
            ClientMessage request = ClientSupplyClassLoadCodec.encodeRequest(className, classDefinition);
            new ClientInvocation(clientInstance, request, member.getAddress()).invoke();
        }
    }

}
