/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class ClientNonSmartListenerService extends ClientListenerServiceImpl {

    private final Map<ClientRegistrationKey, ClientEventRegistration> registrations
            = new ConcurrentHashMap<ClientRegistrationKey, ClientEventRegistration>();

    public ClientNonSmartListenerService(HazelcastClientInstanceImpl client,
                                         int eventThreadCount, int eventQueueCapacity) {
        super(client, eventThreadCount, eventQueueCapacity);
    }

    @Override
    public String registerListener(final ListenerMessageCodec codec, final EventHandler handler) {
        Future<String> future = registrationExecutor.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                String userRegistrationId = UuidUtil.newUnsecureUuidString();
                ClientRegistrationKey registrationKey = new ClientRegistrationKey(userRegistrationId, handler, codec);
                try {
                    ClientEventRegistration registration = invoke(registrationKey);
                    registrations.put(registrationKey, registration);
                } catch (Exception e) {
                    throw new HazelcastException("Listener can not be added", e);
                }
                return userRegistrationId;
            }
        });
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private ClientEventRegistration invoke(ClientRegistrationKey registrationKey) throws Exception {
        EventHandler handler = registrationKey.getHandler();
        handler.beforeListenerRegister();
        ClientMessage request = registrationKey.getCodec().encodeAddRequest(false);
        ClientInvocation invocation = new ClientInvocation(client, request);
        invocation.setEventHandler(handler);

        ClientInvocationFuture future = invocation.invoke();
        String registrationId = registrationKey.getCodec().decodeAddResponse(future.get());
        handler.onListenerRegister();
        Address address = future.getInvocation().getSendConnection().getRemoteEndpoint();
        Member member = client.getClientClusterService().getMember(address);
        return new ClientEventRegistration(registrationId,
                request.getCorrelationId(), member, registrationKey.getCodec());

    }

    @Override
    public boolean deregisterListener(final String userRegistrationId) {
        Future<Boolean> future = registrationExecutor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                ClientRegistrationKey key = new ClientRegistrationKey(userRegistrationId);
                ClientEventRegistration registration = registrations.get(key);

                if (registration == null) {
                    return false;
                }

                ClientMessage request = registration.getCodec().encodeRemoveRequest(registration.getServerRegistrationId());
                try {
                    Future future = new ClientInvocation(client, request).invoke();
                    future.get();
                    removeEventHandler(registration.getCallId());
                    registrations.remove(key);
                } catch (Exception e) {
                    throw new HazelcastException("Listener with id " + userRegistrationId + " could not be removed", e);
                }
                return true;
            }
        });
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public void start() {
        List<Member> allClusterMembers = new ArrayList<Member>(client.getCluster().getMembers());
        new ReconnectionHandler(allClusterMembers).run();
    }

    @Override
    public void onClusterConnect(final ClientConnection clientConnection) {
        registrationExecutor.submit(new ReconnectionHandler(clientConnection.getClientUnregisteredMembers()));
    }

    //For Testing
    public Collection<ClientEventRegistration> getActiveRegistrations(final String uuid) {
        Future<Collection<ClientEventRegistration>> future = registrationExecutor.submit(
                new Callable<Collection<ClientEventRegistration>>() {
                    @Override
                    public Collection<ClientEventRegistration> call() throws Exception {
                        ClientEventRegistration registration = registrations.get(new ClientRegistrationKey(uuid));
                        if (registration == null) {
                            return Collections.EMPTY_LIST;
                        }
                        LinkedList<ClientEventRegistration> activeRegistrations = new LinkedList<ClientEventRegistration>();
                        if (getEventHandler(registration.getCallId()) != null) {
                            activeRegistrations.add(registration);
                        }
                        return activeRegistrations;
                    }
                });
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private class ReconnectionHandler
            implements Runnable {
        private List<Member> clientUnregisteredMembers;

        public ReconnectionHandler(List<Member> clientUnregisteredMembers) {
            this.clientUnregisteredMembers = clientUnregisteredMembers;
        }

        @Override
        public void run() {
            if (registrations.isEmpty()) {
                return;
            }

            if (checkReconnectionToTheSameMember()) {
                return;
            }

            reRegisterListeners();
        }

        private void reRegisterListeners() {
            for (Map.Entry<ClientRegistrationKey, ClientEventRegistration> existingRegistrationEntry : registrations.entrySet()) {
                ClientRegistrationKey key = null;
                try {
                    ClientEventRegistration existingRegistration = existingRegistrationEntry.getValue();
                    removeEventHandler(existingRegistration.getCallId());
                    key = existingRegistrationEntry.getKey();
                    ClientEventRegistration registration = invoke(key);
                    registrations.put(key, registration);
                } catch (Exception e) {
                    logger.warning("Listener " + key + " could not be added ");
                }
            }
        }

        private boolean checkReconnectionToTheSameMember() {
            ClientClusterService clientClusterService = client.getClientClusterService();
            Address newOwnerAddress = clientClusterService.getOwnerConnectionAddress();
            Member newOwnerMember = clientClusterService.getMember(newOwnerAddress);
            ClientEventRegistration firstRegistration = registrations.values().iterator().next();
            // Since this is non-smart client, all registrations are made against the same member
            Address oldOwnerAddress = firstRegistration.getSubscriber().getAddress();
            if (newOwnerAddress.equals(oldOwnerAddress)) {
                // connected to the same member as the owner
                boolean ownerCleanedup = false;
                for (Member member : clientUnregisteredMembers) {
                    if (newOwnerMember.equals(member)) {
                        ownerCleanedup = true;
                        break;
                    }
                }
                if (!ownerCleanedup) {
                    // TODO: what if the cleanup starts later, how do we protect it?
                    return true;
                }
            }
            return false;
        }
    }
}
