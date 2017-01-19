/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ClientSmartListenerService extends ClientListenerServiceImpl implements ConnectionListener {

    private final Map<ClientRegistrationKey, Map<Connection, ClientEventRegistration>> registrations
            = new ConcurrentHashMap<ClientRegistrationKey, Map<Connection, ClientEventRegistration>>();
    private final ClientConnectionManager clientConnectionManager;

    public ClientSmartListenerService(HazelcastClientInstanceImpl client,
                                      int eventThreadCount, int eventQueueCapacity) {
        super(client, eventThreadCount, eventQueueCapacity);
        clientConnectionManager = client.getConnectionManager();
    }

    @Override
    public String registerListener(final ListenerMessageCodec codec, final EventHandler handler) {
        Future<String> future = registrationExecutor.submit(new Callable<String>() {
            @Override
            public String call() {
                String userRegistrationId = UuidUtil.newUnsecureUuidString();

                ClientRegistrationKey registrationKey = new ClientRegistrationKey(userRegistrationId, handler, codec);
                registrations.put(registrationKey, new ConcurrentHashMap<Connection, ClientEventRegistration>());
                try {
                    Collection<ClientConnection> connections = clientConnectionManager.getActiveConnections();
                    for (ClientConnection connection : connections) {
                        invoke(registrationKey, connection);
                    }
                } catch (Exception e) {
                    deregisterListener(userRegistrationId);
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

    private void invoke(ClientRegistrationKey registrationKey, Connection connection) throws Exception {
        Map<Connection, ClientEventRegistration> registrationMap = registrations.get(registrationKey);
        if (registrationMap.containsKey(connection)) {
            return;
        }

        ListenerMessageCodec codec = registrationKey.getCodec();
        ClientMessage request = codec.encodeAddRequest(true);
        EventHandler handler = registrationKey.getHandler();
        handler.beforeListenerRegister();

        ClientInvocation invocation = new ClientInvocation(client, request, connection);
        invocation.setEventHandler(handler);
        String serverRegistrationId = codec.decodeAddResponse(invocation.invoke().get());

        handler.onListenerRegister();
        long correlationId = request.getCorrelationId();
        ClientEventRegistration registration
                = new ClientEventRegistration(serverRegistrationId, correlationId, connection, codec);

        registrationMap.put(connection, registration);
    }

    @Override
    public boolean deregisterListener(final String userRegistrationId) {
        Future<Boolean> future = registrationExecutor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                ClientRegistrationKey key = new ClientRegistrationKey(userRegistrationId);
                Map<Connection, ClientEventRegistration> registrationMap = registrations.get(key);
                if (registrationMap == null) {
                    return false;
                }
                boolean successful = true;
                for (ClientEventRegistration registration : registrationMap.values()) {
                    Connection subscriber = registration.getSubscriber();
                    try {
                        ListenerMessageCodec listenerMessageCodec = registration.getCodec();
                        String serverRegistrationId = registration.getServerRegistrationId();
                        ClientMessage request = listenerMessageCodec.encodeRemoveRequest(serverRegistrationId);
                        new ClientInvocation(client, request, subscriber).invoke().get();
                        removeEventHandler(registration.getCallId());
                        registrationMap.remove(subscriber);
                    } catch (Exception e) {
                        successful = false;
                        logger.warning("Deregistration of listener with id " + userRegistrationId
                                + " has failed to address " + subscriber.getEndPoint(), e);
                    }
                }
                if (successful) {
                    registrations.remove(key);
                }
                return successful;
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
        clientConnectionManager.addConnectionListener(this);
        final ClientClusterService clientClusterService = client.getClientClusterService();
        registrationExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                Collection<Member> memberList = clientClusterService.getMemberList();
                for (Member member : memberList) {
                    clientConnectionManager.getOrTriggerConnect(member.getAddress(), false);
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void connectionAdded(final Connection connection) {
        registrationExecutor.submit(new Runnable() {
            @Override
            public void run() {
                for (ClientRegistrationKey registrationKey : registrations.keySet()) {
                    try {
                        invoke(registrationKey, connection);
                    } catch (Exception e) {
                        logger.warning("Listener " + registrationKey + " can not be added to new connection: "
                                + connection, e);
                    }
                }
            }
        });
    }

    @Override
    public void connectionRemoved(final Connection connection) {
        registrationExecutor.submit(new Runnable() {
            @Override
            public void run() {
                for (Map<Connection, ClientEventRegistration> registrationMap : registrations.values()) {
                    ClientEventRegistration registration = registrationMap.remove(connection);
                    if (registration != null) {
                        removeEventHandler(registration.getCallId());
                    }
                }
            }
        });
    }

    //For Testing
    public Collection<ClientEventRegistration> getActiveRegistrations(final String uuid) {
        Future<Collection<ClientEventRegistration>> future = registrationExecutor.submit(
                new Callable<Collection<ClientEventRegistration>>() {
                    @Override
                    public Collection<ClientEventRegistration> call() {
                        ClientRegistrationKey key = new ClientRegistrationKey(uuid);
                        Map<Connection, ClientEventRegistration> registrationMap = registrations.get(key);
                        if (registrationMap == null) {
                            return Collections.EMPTY_LIST;
                        }
                        LinkedList<ClientEventRegistration> activeRegistrations = new LinkedList<ClientEventRegistration>();
                        for (ClientEventRegistration registration : registrationMap.values()) {
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

}
