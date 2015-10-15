/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.spi.impl.listener;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.BaseClientAddListenerRequest;
import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.nio.Connection;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class ClientNonSmartListenerService extends ClientListenerServiceImpl implements ClientListenerService {

    private final Map<ClientRegistrationKey, ClientEventRegistration> registrations
            = new ConcurrentHashMap<ClientRegistrationKey, ClientEventRegistration>();

    public ClientNonSmartListenerService(HazelcastClientInstanceImpl client, int eventThreadCount, int eventQueueCapacity) {
        super(client, eventThreadCount, eventQueueCapacity);
    }


    @Override
    public String startListening(BaseClientAddListenerRequest request, EventHandler handler) {
        handler.beforeListenerRegister();

        String userRegistrationId = UuidUtil.newUnsecureUuidString();
        ClientRegistrationKey registrationKey = new ClientRegistrationKey(userRegistrationId, request, handler);
        invoke(registrationKey);
        return userRegistrationId;
    }

    public void invoke(ClientRegistrationKey registrationKey) {
        final EventHandler handler = registrationKey.getHandler();
        final ClientRequest request = registrationKey.getRequest();
        ClientInvocation invocation = new ClientInvocation(client, handler, request);
        try {
            ClientInvocationFuture future = invocation.invoke();
            String registrationId = serializationService.toObject(future.get());

            ClientConnection sendConnection = future.getInvocation().getSendConnection();
            ClientEventRegistration registration = new ClientEventRegistration(registrationId,
                    request.getCallId(), sendConnection);
            registerListener(registrationKey, registration);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean stopListening(BaseClientRemoveListenerRequest request, String userRegistrationId) {
        ClientEventRegistration registration = registrations.remove(new ClientRegistrationKey(userRegistrationId));
        if (registration == null) {
            return false;
        }
        removeEventHandler(registration.getCallId());
        request.setRegistrationId(registration.getServerRegistrationId());
        try {
            Future future = new ClientInvocation(client, request, registration.getSubscriber()).invoke();
            future.get();
            return true;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public void registerListener(ClientRegistrationKey key, ClientEventRegistration registration) {
        registrations.put(key, registration);
    }

    @Override
    public void connectionAdded(Connection connection) {
        //ignore
    }

    @Override
    public void connectionRemoved(Connection connection) {
        Set<ClientRegistrationKey> keys = new HashSet<ClientRegistrationKey>(registrations.keySet());
        for (ClientEventRegistration registration : registrations.values()) {
            removeEventHandler(registration.getCallId());
        }
        registrations.clear();
        for (ClientRegistrationKey registrationKey : keys) {
            invoke(registrationKey);
        }
    }
}
