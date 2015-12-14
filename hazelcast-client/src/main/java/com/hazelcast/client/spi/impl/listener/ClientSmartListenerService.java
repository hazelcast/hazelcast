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
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.util.UuidUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class ClientSmartListenerService extends ClientListenerServiceImpl implements InitialMembershipListener {

    private final Set<Member> members = new HashSet<Member>();
    private final Map<ClientRegistrationKey, Map<Address, ClientEventRegistration>> registrations
            = new ConcurrentHashMap<ClientRegistrationKey, Map<Address, ClientEventRegistration>>();
    private final Object listenerRegLock = new Object();
    private String membershipListenerId;

    public ClientSmartListenerService(HazelcastClientInstanceImpl client,
                                      int eventThreadCount, int eventQueueCapacity) {
        super(client, eventThreadCount, eventQueueCapacity);
    }

    @Override
    public String registerListener(ListenerMessageCodec codec, EventHandler handler) {
        String userRegistrationId = UuidUtil.newUnsecureUuidString();
        synchronized (listenerRegLock) {

            ClientRegistrationKey registrationKey = new ClientRegistrationKey(userRegistrationId, handler, codec);
            registrations.put(registrationKey, new ConcurrentHashMap<Address, ClientEventRegistration>());
            try {
                for (Member member : this.members) {
                    invoke(registrationKey, member.getAddress());
                }
            } catch (Exception e) {
                deregisterListener(userRegistrationId);
                throw new HazelcastException("Listener can not be added", e);
            }
            return userRegistrationId;
        }
    }

    private void invoke(ClientRegistrationKey registrationKey, Address address) throws Exception {
        ListenerMessageCodec codec = registrationKey.getCodec();
        ClientMessage request = codec.encodeAddRequest(true);
        EventHandler handler = registrationKey.getHandler();
        handler.beforeListenerRegister();

        ClientInvocation invocation = new ClientInvocation(client, request, address);
        invocation.setEventHandler(handler);
        String serverRegistrationId = codec.decodeAddResponse(invocation.invoke().get());

        handler.onListenerRegister();
        long correlationId = request.getCorrelationId();
        ClientEventRegistration registration
                = new ClientEventRegistration(serverRegistrationId, correlationId, address, codec);

        Map<Address, ClientEventRegistration> registrationMap = registrations.get(registrationKey);
        registrationMap.put(address, registration);

    }

    @Override
    public boolean deregisterListener(String userRegistrationId) {
        synchronized (listenerRegLock) {
            ClientRegistrationKey key = new ClientRegistrationKey(userRegistrationId);
            Map<Address, ClientEventRegistration> registrationMap = registrations.get(key);

            if (registrationMap == null) {
                return false;
            }


            boolean successful = true;
            for (ClientEventRegistration registration : registrationMap.values()) {
                Address subscriber = registration.getSubscriber();
                try {
                    ListenerMessageCodec listenerMessageCodec = registration.getCodec();
                    String serverRegistrationId = registration.getServerRegistrationId();
                    ClientMessage request = listenerMessageCodec.encodeRemoveRequest(serverRegistrationId);
                    Future future = new ClientInvocation(client, request, subscriber).invoke();
                    future.get();
                    removeEventHandler(registration.getCallId());
                    registrationMap.remove(subscriber);
                } catch (Exception e) {
                    successful = false;
                    logger.warning("Deregistration of listener with id " + userRegistrationId
                            + " has failed to address " + subscriber, e);
                }
            }

            if (successful) {
                registrations.remove(key);
            }
            return successful;
        }

    }

    @Override
    public void start() {
        membershipListenerId = client.getClientClusterService().addMembershipListener(this);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        if (membershipListenerId != null) {
            client.getClientClusterService().removeMembershipListener(membershipListenerId);
        }
    }

    @Override
    public void memberAdded(final MembershipEvent membershipEvent) {
        executionService.executeInternal(new Runnable() {
            @Override
            public void run() {
                synchronized (listenerRegLock) {
                    Member member = membershipEvent.getMember();
                    members.add(member);
                    for (ClientRegistrationKey registrationKey : registrations.keySet()) {
                        try {
                            invoke(registrationKey, member.getAddress());
                        } catch (Exception e) {
                            logger.warning("Listener " + registrationKey + " can not added to new member " + member);
                        }
                    }
                }
            }
        });
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        synchronized (listenerRegLock) {
            Member member = membershipEvent.getMember();
            members.remove(member);
            for (Map<Address, ClientEventRegistration> registrationMap : registrations.values()) {
                ClientEventRegistration registration = registrationMap.remove(member.getAddress());
                removeEventHandler(registration.getCallId());
            }

        }
    }

    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        //nothing to do
    }

    @Override
    public void init(InitialMembershipEvent event) {
        synchronized (listenerRegLock) {
            members.addAll(event.getMembers());
            for (Member member : members) {
                for (ClientRegistrationKey registrationKey : registrations.keySet()) {
                    try {
                        invoke(registrationKey, member.getAddress());
                    } catch (Exception e) {
                        logger.warning("Listener " + registrationKey + " can not added to new member " + member);
                    }
                }
            }
        }
    }

    //For Testing
    public Collection<ClientEventRegistration> getActiveRegistrations(String uuid) {
        synchronized (listenerRegLock) {
            Map<Address, ClientEventRegistration> registrationMap = registrations.get(new ClientRegistrationKey(uuid));
            if (registrationMap == null) {
                return Collections.EMPTY_LIST;
            }
            LinkedList<ClientEventRegistration> activeRegistrations = new LinkedList<ClientEventRegistration>();
            for (ClientEventRegistration registration : registrationMap.values()) {
                for (Member member : members) {
                    if (member.getAddress().equals(registration.getSubscriber())) {
                        activeRegistrations.add(registration);
                    }
                }
            }
            return activeRegistrations;
        }
    }

}
