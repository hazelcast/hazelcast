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

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.BaseClientAddListenerRequest;
import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.UuidUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class ClientNonSmartListenerService extends ClientListenerServiceImpl {

    private final Map<ClientRegistrationKey, ClientEventRegistration> registrations =
            new ConcurrentHashMap<ClientRegistrationKey, ClientEventRegistration>();

    public ClientNonSmartListenerService(HazelcastClientInstanceImpl client,
                                         int eventThreadCount, int eventQueueCapacity) {
        super(client, eventThreadCount, eventQueueCapacity);
    }

    @Override
    public String registerListener(BaseClientAddListenerRequest request, EventHandler handler) {
        handler.beforeListenerRegister();

        String userRegistrationId = UuidUtil.newUnsecureUuidString();
        ClientRegistrationKey registrationKey = new ClientRegistrationKey(userRegistrationId, request, handler);
        invoke(registrationKey);
        return userRegistrationId;
    }

    public void invoke(ClientRegistrationKey registrationKey) {
        EventHandler handler = registrationKey.getHandler();
        ClientRequest request = registrationKey.getRequest();
        ClientInvocation invocation = new ClientInvocation(client, handler, request);
        try {
            ClientInvocationFuture future = invocation.invoke();
            String registrationId = serializationService.toObject(future.get());
            handler.onListenerRegister();
            Address address = future.getInvocation().getSendConnection().getRemoteEndpoint();
            ClientEventRegistration registration = new ClientEventRegistration(registrationId,
                    request.getCallId(), address);
            registrations.put(registrationKey, registration);
        } catch (Exception e) {
            //if invocation cannot be done that means connection is broken and there is no need to add listener
            EmptyStatement.ignore(e);
        }
    }

    @Override
    public boolean deregisterListener(BaseClientRemoveListenerRequest request, String userRegistrationId) {
        ClientEventRegistration registration = registrations.remove(new ClientRegistrationKey(userRegistrationId));
        if (registration == null) {
            return false;
        }
        removeEventHandler(registration.getCallId());
        request.setRegistrationId(registration.getServerRegistrationId());
        try {
            Future future = new ClientInvocation(client, request, registration.getSubscriber()).invoke();
            future.get();
        } catch (Exception e) {
            //if invocation cannot be done that means connection is broken and listener is already removed
            EmptyStatement.ignore(e);
        }
        return true;
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        Address ownerConnectionAddress = client.getClientClusterService().getOwnerConnectionAddress();
        if (membershipEvent.getMember().getAddress().equals(ownerConnectionAddress)) {
            executionService.executeInternal(new Runnable() {
                @Override
                public void run() {
                    for (ClientRegistrationKey registrationKey : registrations.keySet()) {
                        invoke(registrationKey);
                    }
                }
            });
        }
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        Address ownerConnectionAddress = client.getClientClusterService().getOwnerConnectionAddress();
        if (membershipEvent.getMember().getAddress().equals(ownerConnectionAddress)) {
            for (ClientEventRegistration registration : registrations.values()) {
                removeEventHandler(registration.getCallId());
            }
        }
    }

    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        //ignore
    }

}
