/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.spi.impl.listener;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.config.RoutingMode;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.ClientListenerService;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.IterableUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.executor.SingleExecutorThreadFactory;
import com.hazelcast.internal.util.executor.StripedExecutor;
import com.hazelcast.internal.util.executor.StripedRunnable;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLIENT_METRIC_LISTENER_SERVICE_EVENTS_PROCESSED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLIENT_METRIC_LISTENER_SERVICE_EVENT_QUEUE_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLIENT_PREFIX_LISTENERS;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class ClientListenerServiceImpl
        implements ClientListenerService, StaticMetricsProvider, ConnectionListener<ClientConnection> {

    private static final String ACTIVE_MULTI_MEMBER_CONNECTIONS = "activeMultiMemberConnection";
    private final HazelcastClientInstanceImpl client;
    private final Map<UUID, ClientListenerRegistration> registrations = new ConcurrentHashMap<>();
    private final Map<String, ClientConnection> multiMemberRoutingListeners = new ConcurrentHashMap<>();

    private final ClientConnectionManager clientConnectionManager;
    private final ILogger logger;
    private final ExecutorService registrationExecutor;
    private final StripedExecutor eventExecutor;
    private final RoutingMode routingMode;


    public ClientListenerServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(ClientListenerService.class);
        String name = client.getName();
        HazelcastProperties properties = client.getProperties();
        int eventQueueCapacity = properties.getInteger(ClientProperty.EVENT_QUEUE_CAPACITY);
        int eventThreadCount = properties.getInteger(ClientProperty.EVENT_THREAD_COUNT);
        this.eventExecutor = new StripedExecutor(logger, name + ".event", eventThreadCount, eventQueueCapacity, true);
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        ThreadFactory threadFactory = new SingleExecutorThreadFactory(classLoader, name + ".eventRegistration-");
        this.registrationExecutor = Executors.newSingleThreadExecutor(threadFactory);
        this.clientConnectionManager = client.getConnectionManager();
        this.routingMode = clientConnectionManager.getRoutingMode();
    }

    @Nonnull
    @Override
    public UUID registerListener(final ListenerMessageCodec codec, final EventHandler handler) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        Future<UUID> future = registrationExecutor.submit(() -> {
            UUID userRegistrationId = UuidUtil.newUnsecureUUID();

            ClientListenerRegistration registration = new ClientListenerRegistration(handler, codec);
            registrations.put(userRegistrationId, registration);
            Collection<ClientConnection> connections = clientConnectionManager.getActiveConnections();

            switch (routingMode) {
                case MULTI_MEMBER -> {
                    ClientConnection activeConnection = getOrUpdateMultiMemberModeListenerRegistrationConnection();
                    doRemoteRegistrationSync(registration, activeConnection, userRegistrationId);
                }
                case SINGLE_MEMBER -> {
                    ClientConnection firstConnection = IterableUtil.getFirst(connections, null);
                    if (firstConnection != null) {
                        doRemoteRegistrationSync(registration, firstConnection, userRegistrationId);
                    }
                }
                case ALL_MEMBERS -> connections.forEach(connection ->
                        doRemoteRegistrationSync(registration, connection, userRegistrationId));
                default -> throw new IllegalArgumentException("Unsupported routing mode: " + routingMode);
            }

            return userRegistrationId;
        });
        try {
            return future.get();
        } catch (
                Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private void doRemoteRegistrationSync(ClientListenerRegistration registration, ClientConnection connection,
                                          UUID userRegistrationId) {
        try {
            invoke(registration, connection);
        } catch (Exception e) {
            if (connection.isAlive()) {
                deregisterListenerInternal(userRegistrationId);
                throw new HazelcastException("Listener can not be added ", e);
            }
        }
    }

    @Override
    public boolean deregisterListener(@Nullable UUID userRegistrationId) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));
        checkNotNull(userRegistrationId, "Null userRegistrationId is not allowed!");

        try {
            Future<Boolean> future = registrationExecutor.submit(() -> deregisterListenerInternal(userRegistrationId));

            try {
                return future.get();
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        } catch (RejectedExecutionException ignored) {
            //RejectedExecutionException executor(hence the client) is already shutdown
            //listeners are cleaned up by the server side. We can ignore the exception and return true safely
            EmptyStatement.ignore(ignored);
            return true;
        }

    }

    @Override
    public void provideStaticMetrics(MetricsRegistry registry) {
        registry.registerStaticMetrics(this, CLIENT_PREFIX_LISTENERS);
    }

    @Probe(name = CLIENT_METRIC_LISTENER_SERVICE_EVENT_QUEUE_SIZE, level = MANDATORY)
    private int eventQueueSize() {
        return eventExecutor.getWorkQueueSize();
    }

    @Probe(name = CLIENT_METRIC_LISTENER_SERVICE_EVENTS_PROCESSED, level = MANDATORY)
    private long eventsProcessed() {
        return eventExecutor.processedCount();
    }

    public void handleEventMessage(ClientMessage clientMessage) {
        Runnable eventProcessor;
        if (clientMessage.getPartitionId() == -1) {
            // Execute on a random worker
            eventProcessor = () -> handleEventMessageOnCallingThread(clientMessage);
        } else {
            eventProcessor = new ClientEventProcessor(clientMessage);
        }
        try {
            eventExecutor.execute(eventProcessor);
        } catch (RejectedExecutionException e) {
            logger.warning("Event clientMessage could not be handled", e);
        }
    }

    public void handleEventMessageOnCallingThread(ClientMessage clientMessage) {
        long correlationId = clientMessage.getCorrelationId();
        ClientConnection connection = (ClientConnection) clientMessage.getConnection();
        EventHandler eventHandler = connection.getEventHandler(correlationId);
        if (eventHandler == null) {
            logger.warning("No eventHandler for callId: " + correlationId + " event: " + clientMessage);
            return;
        }
        eventHandler.handle(clientMessage);
    }

    protected void invoke(ClientListenerRegistration listenerRegistration, ClientConnection connection) throws Exception {
        //This method should only be called from registrationExecutor
        assert (Thread.currentThread().getName().contains("eventRegistration"));

        if (listenerRegistration.getConnectionRegistrations().containsKey(connection)) {
            return;
        }

        ListenerMessageCodec codec = listenerRegistration.getCodec();
        ClientMessage request = codec.encodeAddRequest(registersLocalOnly());
        EventHandler handler = listenerRegistration.getHandler();
        if (logger.isFinestEnabled()) {
            logger.finest("Register attempt of %s to %s", listenerRegistration, connection);
        }
        handler.beforeListenerRegister(connection);

        ClientInvocation invocation = new ClientInvocation(client, request, null, connection);
        invocation.setEventHandler(handler);
        ClientInvocationFuture future = invocation.invokeUrgent();

        ClientMessage clientMessage;
        try {
            clientMessage = future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e, Exception.class);
        }

        UUID serverRegistrationId = codec.decodeAddResponse(clientMessage);
        if (logger.isFinestEnabled()) {
            logger.finest("Registered %s to %s", listenerRegistration, connection);
        }
        handler.onListenerRegister(connection);
        long correlationId = request.getCorrelationId();
        ClientConnectionRegistration registration
                = new ClientConnectionRegistration(serverRegistrationId, correlationId);

        listenerRegistration.getConnectionRegistrations().put(connection, registration);
    }

    @Override
    public void connectionAdded(final ClientConnection connection) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        registrationExecutor.submit(() -> {
            if (routingMode == RoutingMode.MULTI_MEMBER) {
                getOrUpdateMultiMemberModeListenerRegistrationConnection();
            } else {
                for (ClientListenerRegistration listenerRegistration : registrations.values()) {
                    invokeFromInternalThread(listenerRegistration, connection);
                }
            }
        });
    }

    public void shutdown() {
        eventExecutor.shutdown();
        registrationExecutor.shutdown();
        ClientExecutionServiceImpl.awaitExecutorTermination("registrationExecutor", registrationExecutor, logger);
    }

    public void start() {
        clientConnectionManager.addConnectionListener(this);
    }

    @Override
    public void connectionRemoved(final ClientConnection connection) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        registrationExecutor.submit(() -> {
            if (routingMode == RoutingMode.MULTI_MEMBER) {
                boolean removed = multiMemberRoutingListeners.remove(ACTIVE_MULTI_MEMBER_CONNECTIONS, connection);
                if (removed) {
                    // if we removed the responsible connection
                    // for listener registration in subset mode,
                    // we need to assign a new connection with
                    // `getOrUpdateMultiMemberModeListenerRegistrationConnection`
                    // method
                    getOrUpdateMultiMemberModeListenerRegistrationConnection();
                }
            }
            registrations.values().forEach(registry -> registry.getConnectionRegistrations().remove(connection));
        });
    }

    private ClientConnection getOrUpdateMultiMemberModeListenerRegistrationConnection() {
        return multiMemberRoutingListeners.computeIfAbsent(ACTIVE_MULTI_MEMBER_CONNECTIONS, k -> {
            ClientConnection randomConnection = IterableUtil.getFirst(clientConnectionManager.getActiveConnections(),
                    null);
            if (randomConnection != null) {
                for (ClientListenerRegistration listenerRegistration : registrations.values()) {
                    invokeFromInternalThread(listenerRegistration, randomConnection);
                }
            }
            return randomConnection;
        });
    }

    //called from ee.
    public StripedExecutor getEventExecutor() {
        return eventExecutor;
    }

    //For Testing
    public Map<ClientConnection, ClientConnectionRegistration> getActiveRegistrations(final UUID uuid) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        Future<Map<ClientConnection, ClientConnectionRegistration>> future = registrationExecutor.submit(() -> {
            ClientListenerRegistration listenerRegistration = registrations.get(uuid);
            if (listenerRegistration == null) {
                return Collections.emptyMap();
            }
            return listenerRegistration.getConnectionRegistrations();
        });
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    // used in tests
    public Map<UUID, ClientListenerRegistration> getRegistrations() {
        return Collections.unmodifiableMap(registrations);
    }

    private void invokeFromInternalThread(ClientListenerRegistration registrationKey, ClientConnection connection) {
        //This method should only be called from registrationExecutor
        assert (Thread.currentThread().getName().contains("eventRegistration"));

        try {
            invoke(registrationKey, connection);
        } catch (Exception e) {
            logger.warning("Listener " + registrationKey + " can not be added to a new connection: "
                    + connection + ", reason: " + e.getMessage());
        }
    }

    private boolean registersLocalOnly() {
        return routingMode == RoutingMode.ALL_MEMBERS;
    }

    private Boolean deregisterListenerInternal(@Nullable UUID userRegistrationId) {
        //This method should only be called from registrationExecutor
        assert (Thread.currentThread().getName().contains("eventRegistration"));

        ClientListenerRegistration listenerRegistration = registrations.remove(userRegistrationId);
        if (listenerRegistration == null) {
            return false;
        }

        Map<ClientConnection, ClientConnectionRegistration> registrations = listenerRegistration.getConnectionRegistrations();
        CompletableFuture[] futures = new CompletableFuture[registrations.size()];
        int i = 0;
        for (Map.Entry<ClientConnection, ClientConnectionRegistration> entry : registrations.entrySet()) {
            ClientConnectionRegistration registration = entry.getValue();
            ClientConnection subscriber = entry.getKey();
            //remove local handler
            subscriber.removeEventHandler(registration.getCallId());
            //the rest is for deleting remote registration
            ListenerMessageCodec listenerMessageCodec = listenerRegistration.getCodec();
            UUID serverRegistrationId = registration.getServerRegistrationId();
            ClientMessage request = listenerMessageCodec.encodeRemoveRequest(serverRegistrationId);
            if (request == null) {
                futures[i++] = CompletableFuture.completedFuture(null);
                continue;
            }
            ClientInvocation clientInvocation = new ClientInvocation(client, request, null, subscriber);
            clientInvocation.setInvocationTimeoutMillis(Long.MAX_VALUE);
            futures[i++] = clientInvocation.invokeUrgent().handleAsync((response, throwable) -> {
                if (throwable == null) {
                    boolean result = listenerMessageCodec.decodeRemoveResponse(response);
                    if (!result) {
                        logger.warning("Deregistration of listener with ID " + userRegistrationId
                                + " has failed for address " + subscriber.getRemoteAddress() + " with no exception");
                    }
                    return null;
                }

                if (!(throwable instanceof HazelcastClientNotActiveException
                        || throwable instanceof IOException
                        || throwable instanceof TargetDisconnectedException)) {
                    logger.warning("Deregistration of listener with ID " + userRegistrationId
                            + " has failed for address " + subscriber.getRemoteAddress(), throwable);
                }

                return null;
            }, ConcurrencyUtil.getDefaultAsyncExecutor());
        }
        CompletableFuture.allOf(futures).join();
        return true;
    }

    public boolean removeListener(UUID userRegistrationId) {
        try {
            return registrationExecutor.submit(() -> {
                ClientListenerRegistration listenerRegistration = registrations.remove(userRegistrationId);
                if (listenerRegistration == null) {
                    return false;
                }
                listenerRegistration.getConnectionRegistrations().forEach((connection, registration) ->
                        connection.removeEventHandler(registration.getCallId()));
                return true;
            }).get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private final class ClientEventProcessor implements StripedRunnable {
        final ClientMessage clientMessage;

        private ClientEventProcessor(ClientMessage clientMessage) {
            this.clientMessage = clientMessage;
        }

        @Override
        public void run() {
            handleEventMessageOnCallingThread(clientMessage);
        }

        @Override
        public int getKey() {
            return clientMessage.getPartitionId();
        }
    }
}
