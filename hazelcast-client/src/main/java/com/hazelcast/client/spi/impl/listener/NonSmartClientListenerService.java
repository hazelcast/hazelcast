/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class NonSmartClientListenerService extends AbstractClientListenerService implements ConnectionListener {

    private final Map<ClientRegistrationKey, ClientEventRegistration> activeRegistrations
            = new ConcurrentHashMap<ClientRegistrationKey, ClientEventRegistration>();
    private final Set<ClientRegistrationKey> userRegistrations = new HashSet<ClientRegistrationKey>();

    public NonSmartClientListenerService(HazelcastClientInstanceImpl client,
                                         int eventThreadCount, int eventQueueCapacity) {
        super(client, eventThreadCount, eventQueueCapacity);
    }

    @Override
    public String registerListener(final ListenerMessageCodec codec, final EventHandler handler) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        Future<String> future = registrationExecutor.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                String userRegistrationId = UuidUtil.newUnsecureUuidString();
                ClientRegistrationKey registrationKey = new ClientRegistrationKey(userRegistrationId, handler, codec);
                try {
                    ClientEventRegistration registration = invoke(registrationKey);
                    activeRegistrations.put(registrationKey, registration);
                    userRegistrations.add(registrationKey);
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
        //This method should only be called from registrationExecutor
        assert (Thread.currentThread().getName().contains("eventRegistration"));

        EventHandler handler = registrationKey.getHandler();
        handler.beforeListenerRegister();
        ClientMessage request = registrationKey.getCodec().encodeAddRequest(false);
        ClientInvocation invocation = new ClientInvocation(client, request, null);
        invocation.setEventHandler(handler);

        ClientInvocationFuture future = invocation.invoke();
        String registrationId = registrationKey.getCodec().decodeAddResponse(future.get());
        handler.onListenerRegister();
        Connection connection = future.getInvocation().getSendConnection();
        return new ClientEventRegistration(registrationId,
                request.getCorrelationId(), connection, registrationKey.getCodec());
    }

    @Override
    public boolean deregisterListener(final String userRegistrationId) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        Future<Boolean> future = registrationExecutor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                ClientRegistrationKey key = new ClientRegistrationKey(userRegistrationId);

                if (!userRegistrations.remove(key)) {
                    return false;
                }

                ClientEventRegistration registration = activeRegistrations.get(key);
                if (registration == null) {
                    return true;
                }

                ClientMessage request = registration.getCodec().encodeRemoveRequest(registration.getServerRegistrationId());
                try {
                    Future future = new ClientInvocation(client, request, null).invoke();
                    future.get();
                    removeEventHandler(registration.getCallId());
                    activeRegistrations.remove(key);
                } catch (Exception e) {
                    throw new HazelcastException("Listener with ID " + userRegistrationId + " could not be removed", e);
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
    public void connectionAdded(final Connection connection) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        registrationExecutor.submit(new Runnable() {
            @Override
            public void run() {
                for (ClientRegistrationKey registrationKey : userRegistrations) {
                    try {
                        ClientEventRegistration oldRegistration = activeRegistrations.get(registrationKey);
                        if (oldRegistration != null) {
                            //if there was a registration corresponding to same user listener key before,
                            //then we need to remove its event handler as cleanup
                            removeEventHandler(oldRegistration.getCallId());
                        }
                        ClientEventRegistration eventRegistration = invoke(registrationKey);
                        activeRegistrations.put(registrationKey, eventRegistration);
                    } catch (Exception e) {
                        logger.warning("Listener " + registrationKey + " can not be added to new connection: "
                                + connection, e);
                    }
                }
            }
        });

    }

    @Override
    public void connectionRemoved(Connection connection) {
    }

    //For Testing
    public Collection<ClientEventRegistration> getActiveRegistrations(final String uuid) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        Future<Collection<ClientEventRegistration>> future = registrationExecutor.submit(
                new Callable<Collection<ClientEventRegistration>>() {
                    @Override
                    public Collection<ClientEventRegistration> call() throws Exception {
                        ClientEventRegistration registration = activeRegistrations.get(new ClientRegistrationKey(uuid));
                        if (registration == null || !registration.getSubscriber().isAlive()) {
                            return Collections.EMPTY_LIST;
                        }
                        LinkedList<ClientEventRegistration> activeRegistrations = new LinkedList<ClientEventRegistration>();
                        activeRegistrations.add(registration);
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
