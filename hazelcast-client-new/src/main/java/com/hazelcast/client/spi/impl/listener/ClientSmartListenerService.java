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
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ClientListenerInvocation;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Connection;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class ClientSmartListenerService extends ClientListenerServiceImpl
        implements ClientListenerService {

    private final Map<ClientRegistrationKey, Set<ClientEventRegistration>> registrations
            = new ConcurrentHashMap<ClientRegistrationKey, Set<ClientEventRegistration>>();
    private final Object regMutex = new Object();

    public ClientSmartListenerService(HazelcastClientInstanceImpl client, int eventThreadCount, int eventQueueCapacity) {
        super(client, eventThreadCount, eventQueueCapacity);
    }

    @Override
    public String startListening(ListenerMessageCodec codec, EventHandler handler) {
        handler.beforeListenerRegister();
        ClientMessage request = codec.encodeAddRequest(true);

        Collection<Member> members = client.getClientClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);

        String userRegistrationId = UuidUtil.newUnsecureUuidString();
        ClientRegistrationKey registrationKey = new ClientRegistrationKey(userRegistrationId, request, handler, codec);
        for (Member member : members) {
            invoke(member, registrationKey);
        }
        return userRegistrationId;
    }

    private void invoke(Member member, ClientRegistrationKey registrationKey) {

        ListenerMessageCodec codec = registrationKey.getCodec();
        ClientMessage oldRequest = registrationKey.getRequest();
        ClientMessage request = ClientMessage.createForDecode(oldRequest.buffer(), 0);

        ClientInvocation invocation =
                new ClientListenerInvocation(client, registrationKey.getHandler(), request, member.getAddress());
        try {
            ClientInvocationFuture future = invocation.invoke();
            String serverRegistrationId = codec.decodeAddResponse(future.get());

            ClientConnection sendConnection = future.getInvocation().getSendConnection();
            ClientEventRegistration registration
                    = new ClientEventRegistration(serverRegistrationId, request.getCorrelationId(), sendConnection, codec);
            registerListener(registrationKey, registration);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private void invoke(Connection connection, ClientRegistrationKey registrationKey) {

        ListenerMessageCodec codec = registrationKey.getCodec();
        ClientMessage oldRequest = registrationKey.getRequest();
        ClientMessage request = ClientMessage.createForDecode(oldRequest.buffer(), 0);

        ClientInvocation invocation =
                new ClientListenerInvocation(client, registrationKey.getHandler(), request, connection);
        try {
            ClientInvocationFuture future = invocation.invoke();
            String serverRegistrationId = codec.decodeAddResponse(future.get());

            ClientConnection sendConnection = future.getInvocation().getSendConnection();
            ClientEventRegistration registration
                    = new ClientEventRegistration(serverRegistrationId, request.getCorrelationId(), sendConnection, codec);
            registerListener(registrationKey, registration);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public void registerListener(ClientRegistrationKey registrationKey, ClientEventRegistration registration) {
        Set<ClientEventRegistration> regSet = registrations.get(registrationKey);
        if (regSet == null) {
            synchronized (regMutex) {
                regSet = registrations.get(registrationKey);
                if (regSet == null) {
                    regSet = Collections.newSetFromMap(new ConcurrentHashMap<ClientEventRegistration, Boolean>());
                }
            }
        }
        regSet.add(registration);
    }


    @Override
    public boolean stopListening(String userRegistrationId) {
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
                throw ExceptionUtil.rethrow(e);
            }
        }
        return true;
    }

    @Override
    public void connectionAdded(Connection connection) {
        for (ClientRegistrationKey registrationKey : registrations.keySet()) {
            invoke(connection, registrationKey);
        }
    }

    @Override
    public void connectionRemoved(Connection connection) {
        for (Set<ClientEventRegistration> eventRegistrations : registrations.values()) {
            removeRegistration(connection, eventRegistrations.iterator());
        }
    }

    private void removeRegistration(Connection connection, Iterator<ClientEventRegistration> iterator) {
        while (iterator.hasNext()) {
            ClientEventRegistration registration = iterator.next();
            if (registration.getSubscriber().equals(connection)) {
                iterator.remove();
                removeEventHandler(registration.getCallId());
                return;
            }
        }
    }
}
