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

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.Address;
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

public class ClientNonSmartListenerService extends ClientListenerServiceImpl implements ConnectionListener {

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
        return new ClientEventRegistration(registrationId,
                request.getCorrelationId(), address, registrationKey.getCodec());

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
        client.getConnectionManager().addConnectionListener(this);
    }

    @Override
    public void connectionAdded(Connection connection) {
        registrationExecutor.submit(new Runnable() {
            @Override
            public void run() {
                for (ClientRegistrationKey registrationKey : registrations.keySet()) {
                    try {
                        ClientEventRegistration registration = invoke(registrationKey);
                        registrations.put(registrationKey, registration);
                    } catch (Exception e) {
                        logger.warning("Listener " + registrationKey + " could not be added ");
                    }
                }
            }
        });

    }

    @Override
    public void connectionRemoved(Connection connection) {
        registrationExecutor.submit(new Runnable() {
            @Override
            public void run() {
                for (Map.Entry<ClientRegistrationKey, ClientEventRegistration> entry : registrations.entrySet()) {
                    removeEventHandler(entry.getValue().getCallId());
                }
            }
        });
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
}
