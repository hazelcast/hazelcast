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

import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.AbstractClientInvocationService;
import com.hazelcast.client.spi.impl.ConnectionHeartbeatListener;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.StringUtil.timeToString;

public class SmartClientListenerService extends AbstractClientListenerService
        implements ConnectionListener, ConnectionHeartbeatListener {

    private final long invocationTimeoutMillis;
    private final long invocationRetryPauseMillis;
    private final Map<ClientRegistrationKey, Map<Connection, ClientEventRegistration>> registrations
            = new ConcurrentHashMap<ClientRegistrationKey, Map<Connection, ClientEventRegistration>>();
    private final ClientConnectionManager clientConnectionManager;
    private final Map<Connection, Collection<ClientRegistrationKey>> failedRegistrations
            = new ConcurrentHashMap<Connection, Collection<ClientRegistrationKey>>();

    public SmartClientListenerService(HazelcastClientInstanceImpl client,
                                      int eventThreadCount, int eventQueueCapacity) {
        super(client, eventThreadCount, eventQueueCapacity);
        clientConnectionManager = client.getConnectionManager();
        AbstractClientInvocationService invocationService = (AbstractClientInvocationService) client.getInvocationService();
        invocationTimeoutMillis = invocationService.getInvocationTimeoutMillis();
        invocationRetryPauseMillis = invocationService.getInvocationRetryPauseMillis();
    }

    @Override
    public String registerListener(final ListenerMessageCodec codec, final EventHandler handler) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        trySyncConnectToAllMembers();

        Future<String> future = registrationExecutor.submit(new Callable<String>() {
            @Override
            public String call() {
                String userRegistrationId = UuidUtil.newUnsecureUuidString();

                ClientRegistrationKey registrationKey = new ClientRegistrationKey(userRegistrationId, handler, codec);
                registrations.put(registrationKey, new ConcurrentHashMap<Connection, ClientEventRegistration>());
                Collection<ClientConnection> connections = clientConnectionManager.getActiveConnections();
                for (ClientConnection connection : connections) {
                    try {
                        invoke(registrationKey, connection);
                    } catch (Exception e) {
                        if (connection.isAlive()) {
                            deregisterListenerInternal(userRegistrationId);
                            throw new HazelcastException("Listener can not be added ", e);
                        }

                    }
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
        //This method should only be called from registrationExecutor
        assert (Thread.currentThread().getName().contains("eventRegistration"));

        Map<Connection, ClientEventRegistration> registrationMap = registrations.get(registrationKey);
        if (registrationMap.containsKey(connection)) {
            return;
        }

        ListenerMessageCodec codec = registrationKey.getCodec();
        ClientMessage request = codec.encodeAddRequest(true);
        EventHandler handler = registrationKey.getHandler();
        handler.beforeListenerRegister();

        ClientInvocation invocation = new ClientInvocation(client, request, null, connection);
        invocation.setEventHandler(handler);
        ClientInvocationFuture future = invocation.invokeUrgent();

        ClientMessage clientMessage;
        try {
            clientMessage = future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e, Exception.class);
        }

        String serverRegistrationId = codec.decodeAddResponse(clientMessage);
        handler.onListenerRegister();
        long correlationId = request.getCorrelationId();
        ClientEventRegistration registration
                = new ClientEventRegistration(serverRegistrationId, correlationId, connection, codec);

        registrationMap.put(connection, registration);
    }

    @Override
    public boolean deregisterListener(final String userRegistrationId) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        Future<Boolean> future = registrationExecutor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return deregisterListenerInternal(userRegistrationId);
            }
        });

        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }

    }

    private Boolean deregisterListenerInternal(String userRegistrationId) {
        //This method should only be called from registrationExecutor
        assert (Thread.currentThread().getName().contains("eventRegistration"));

        ClientRegistrationKey key = new ClientRegistrationKey(userRegistrationId);
        Map<Connection, ClientEventRegistration> registrationMap = registrations.get(key);
        if (registrationMap == null) {
            return false;
        }
        boolean successful = true;

        for (Iterator<ClientEventRegistration> iterator = registrationMap.values().iterator(); iterator.hasNext(); ) {
            ClientEventRegistration registration = iterator.next();
            Connection subscriber = registration.getSubscriber();
            try {
                ListenerMessageCodec listenerMessageCodec = registration.getCodec();
                String serverRegistrationId = registration.getServerRegistrationId();
                ClientMessage request = listenerMessageCodec.encodeRemoveRequest(serverRegistrationId);
                new ClientInvocation(client, request, null, subscriber).invoke().get();
                removeEventHandler(registration.getCallId());
                iterator.remove();
            } catch (Exception e) {
                if (subscriber.isAlive()) {
                    successful = false;
                    logger.warning("Deregistration of listener with ID " + userRegistrationId
                            + " has failed to address " + subscriber.getEndPoint(), e);
                }
            }
        }
        if (successful) {
            registrations.remove(key);
        }
        return successful;
    }

    @Override
    public void start() {
        clientConnectionManager.addConnectionListener(this);
        clientConnectionManager.addConnectionHeartbeatListener(this);
        final ClientClusterService clientClusterService = client.getClientClusterService();
        registrationExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                Collection<Member> memberList = clientClusterService.getMemberList();
                for (Member member : memberList) {
                    try {
                        clientConnectionManager.getOrTriggerConnect(member.getAddress(), false);
                    } catch (IOException e) {
                        return;
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void trySyncConnectToAllMembers() {
        ClientClusterService clientClusterService = client.getClientClusterService();
        long startMillis = System.currentTimeMillis();

        do {
            Member lastFailedMember = null;
            Exception lastException = null;

            for (Member member : clientClusterService.getMemberList()) {
                try {
                    clientConnectionManager.getOrConnect(member.getAddress());
                } catch (Exception e) {
                    lastFailedMember = member;
                    lastException = e;
                }
            }

            if (lastException == null) {
                // successfully connected to all members, break loop.
                break;
            }

            timeOutOrSleepBeforeNextTry(startMillis, lastFailedMember, lastException);

        } while (client.getLifecycleService().isRunning());
    }

    private void sleepBeforeNextTry() {
        try {
            Thread.sleep(invocationRetryPauseMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ExceptionUtil.rethrow(e);
        }
    }

    private void timeOutOrSleepBeforeNextTry(long startMillis, Member lastFailedMember, Exception lastException) {
        long nowInMillis = System.currentTimeMillis();
        long elapsedMillis = nowInMillis - startMillis;
        boolean timedOut = elapsedMillis > invocationTimeoutMillis;

        if (timedOut) {
            throwOperationTimeoutException(startMillis, nowInMillis, elapsedMillis, lastFailedMember, lastException);
        } else {
            sleepBeforeNextTry();
        }
    }

    private void throwOperationTimeoutException(long startMillis, long nowInMillis,
                                                long elapsedMillis, Member lastFailedMember, Exception lastException) {
        throw new OperationTimeoutException("Registering listeners is timed out."
                + " Last failed member : " + lastFailedMember + ", "
                + " Current time: " + timeToString(nowInMillis) + ", "
                + " Start time : " + timeToString(startMillis) + ", "
                + " Client invocation timeout : " + invocationTimeoutMillis + " ms, "
                + " Elapsed time : " + elapsedMillis + " ms. ", lastException);
    }

    @Override
    public void connectionAdded(final Connection connection) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        registrationExecutor.submit(new Runnable() {
            @Override
            public void run() {
                for (ClientRegistrationKey registrationKey : registrations.keySet()) {
                    invokeFromInternalThread(registrationKey, connection);
                }
            }
        });
    }

    @Override
    public void connectionRemoved(final Connection connection) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        registrationExecutor.submit(new Runnable() {
            @Override
            public void run() {
                failedRegistrations.remove(connection);
                for (Map<Connection, ClientEventRegistration> registrationMap : registrations.values()) {
                    ClientEventRegistration registration = registrationMap.remove(connection);
                    if (registration != null) {
                        removeEventHandler(registration.getCallId());
                    }
                }
            }
        });
    }

    @Override
    public void heartbeatResumed(final Connection connection) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        registrationExecutor.submit(new Runnable() {
            @Override
            public void run() {
                Collection<ClientRegistrationKey> registrationKeys = failedRegistrations.get(connection);
                for (ClientRegistrationKey registrationKey : registrationKeys) {
                    invokeFromInternalThread(registrationKey, connection);
                }
            }
        });
    }

    private void invokeFromInternalThread(ClientRegistrationKey registrationKey, Connection connection) {
        //This method should only be called from registrationExecutor
        assert (Thread.currentThread().getName().contains("eventRegistration"));

        try {
            invoke(registrationKey, connection);
        } catch (IOException e) {
            Collection<ClientRegistrationKey> failedRegsToConnection = failedRegistrations.get(connection);
            if (failedRegsToConnection == null) {
                failedRegsToConnection = Collections.newSetFromMap(new HashMap<ClientRegistrationKey, Boolean>());
                failedRegistrations.put(connection, failedRegsToConnection);
            }
            failedRegsToConnection.add(registrationKey);
        } catch (Exception e) {
            logger.warning("Listener " + registrationKey + " can not be added to a new connection: "
                    + connection + ", reason: " + e.getMessage());
        }
    }

    @Override
    public void heartbeatStopped(Connection connection) {
        //no op
    }

    //For Testing
    public Collection<ClientEventRegistration> getActiveRegistrations(final String uuid) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

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

    // used in tests
    public Map<ClientRegistrationKey, Map<Connection, ClientEventRegistration>> getRegistrations() {
        return registrations;
    }
}
