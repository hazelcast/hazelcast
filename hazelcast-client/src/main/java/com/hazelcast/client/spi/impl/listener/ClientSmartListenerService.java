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
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.UuidUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

public class ClientSmartListenerService extends ClientListenerServiceImpl {

    private static final ConstructorFunction<ClientRegistrationKey, Set<ClientEventRegistration>> CONSTRUCTOR_FUNCTION
            = new ConstructorFunction<ClientRegistrationKey, Set<ClientEventRegistration>>() {
        @Override
        public Set<ClientEventRegistration> createNew(ClientRegistrationKey arg) {
            return Collections.newSetFromMap(new ConcurrentHashMap<ClientEventRegistration, Boolean>());
        }
    };
    private final ConcurrentMap<ClientRegistrationKey, Set<ClientEventRegistration>> registrations
            = new ConcurrentHashMap<ClientRegistrationKey, Set<ClientEventRegistration>>();
    private final Object regMutex = new Object();

    public ClientSmartListenerService(HazelcastClientInstanceImpl client, int eventThreadCount, int eventQueueCapacity) {
        super(client, eventThreadCount, eventQueueCapacity);
    }

    @Override
    public String registerListener(ListenerMessageCodec codec, EventHandler handler) {
        ClientMessage request = codec.encodeAddRequest(true);

        Collection<Member> members = client.getClientClusterService().getMemberList();

        String userRegistrationId = UuidUtil.newUnsecureUuidString();
        ClientRegistrationKey registrationKey = new ClientRegistrationKey(userRegistrationId, request, handler, codec);
        for (Member member : members) {
            invoke(registrationKey, member.getAddress());
        }
        return userRegistrationId;
    }

    private void invoke(ClientRegistrationKey registrationKey, Address address) {
        ClientMessage request = registrationKey.getRequest();
        EventHandler handler = registrationKey.getHandler();
        handler.beforeListenerRegister();
        ClientInvocation invocation = new ClientInvocation(client, request, address);
        invocation.setEventHandler(handler);
        ListenerMessageCodec codec = registrationKey.getCodec();
        try {
            ClientInvocationFuture future = invocation.invoke();
            String serverRegistrationId = codec.decodeAddResponse(future.get());
            handler.onListenerRegister();
            int correlationId = request.getCorrelationId();
            ClientEventRegistration registration
                    = new ClientEventRegistration(serverRegistrationId, correlationId, address, codec);
            registerListener(registrationKey, registration);
        } catch (Exception e) {
            //if invocation cannot be done that means connection is broken and there is no need to add listener
            EmptyStatement.ignore(e);
        }
    }

    private void registerListener(ClientRegistrationKey registrationKey, ClientEventRegistration registration) {
        Set<ClientEventRegistration> regSet =
                ConcurrencyUtil.getOrPutSynchronized(registrations, registrationKey, regMutex, CONSTRUCTOR_FUNCTION);
        regSet.add(registration);
    }


    @Override
    public boolean deregisterListener(String userRegistrationId) {
        Set<ClientEventRegistration> regSet = registrations.remove(new ClientRegistrationKey(userRegistrationId));
        if (regSet == null) {
            return false;
        }
        for (ClientEventRegistration registration : regSet) {
            try {
                removeEventHandler(registration.getCallId());
                ListenerMessageCodec listenerMessageCodec = registration.getCodec();
                ClientMessage request = listenerMessageCodec.encodeRemoveRequest(registration.getServerRegistrationId());
                Future future = new ClientInvocation(client, request, registration.getSubscriber()).invoke();
                future.get();
            } catch (Exception e) {
                //if invocation cannot be done that means connection is broken and listener is already removed
                EmptyStatement.ignore(e);
            }
        }
        return true;
    }

    @Override
    public void memberAdded(final MembershipEvent membershipEvent) {
        executionService.executeInternal(new Runnable() {
            @Override
            public void run() {
                for (ClientRegistrationKey registrationKey : registrations.keySet()) {
                    invoke(registrationKey, membershipEvent.getMember().getAddress());
                }
            }
        });
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        for (Set<ClientEventRegistration> eventRegistrations : registrations.values()) {
            removeRegistration(membershipEvent.getMember().getAddress(), eventRegistrations.iterator());
        }
    }

    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        //ignore
    }

    private void removeRegistration(Address address, Iterator<ClientEventRegistration> iterator) {
        while (iterator.hasNext()) {
            ClientEventRegistration registration = iterator.next();
            if (registration.getSubscriber().equals(address)) {
                iterator.remove();
                removeEventHandler(registration.getCallId());
                return;
            }
        }
    }
}
