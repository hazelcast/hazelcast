/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.internal.util.ExceptionUtil;
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

public class ClientListenerServiceImpl implements ClientListenerService, StaticMetricsProvider, ConnectionListener {

    private final HazelcastClientInstanceImpl client;
    private final Map<UUID, ClientListenerRegistration> registrations = new ConcurrentHashMap<>();
    private final ClientConnectionManager clientConnectionManager;
    private final ILogger logger;
    private final ExecutorService registrationExecutor;
    private final StripedExecutor eventExecutor;
    private final boolean isSmart;

    public ClientListenerServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.isSmart = client.getClientConfig().getNetworkConfig().isSmartRouting();
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
            Collection<Connection> connections = clientConnectionManager.getActiveConnections();
            for (Connection connection : connections) {
                try {
                    invoke(registration, connection);
                } catch (Exception e) {
                    if (connection.isAlive()) {
                        deregisterListenerInternal(userRegistrationId);
                        throw new HazelcastException("Listener can not be added ", e);
                    }

                }
            }
            return userRegistrationId;
        });
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
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
            if (logger.isFineEnabled()) {
                logger.fine("No eventHandler for callId: " + correlationId + ", event: " + clientMessage);
            }
            return;
        }

        eventHandler.handle(clientMessage);
    }

    protected void invoke(ClientListenerRegistration listenerRegistration, Connection connection) throws Exception {
        //This method should only be called from registrationExecutor
        assert (Thread.currentThread().getName().contains("eventRegistration"));

        if (listenerRegistration.getConnectionRegistrations().containsKey(connection)) {
            return;
        }

        ListenerMessageCodec codec = listenerRegistration.getCodec();
        ClientMessage request = codec.encodeAddRequest(registersLocalOnly());
        EventHandler handler = listenerRegistration.getHandler();
        if (logger.isFinestEnabled()) {
            logger.finest("Register attempt of " + listenerRegistration + " to " + connection);
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
            logger.finest("Registered " + listenerRegistration + " to " + connection);
        }
        handler.onListenerRegister(connection);
        long correlationId = request.getCorrelationId();
        ClientConnectionRegistration registration
                = new ClientConnectionRegistration(serverRegistrationId, correlationId);

        listenerRegistration.getConnectionRegistrations().put(connection, registration);
    }

    @Override
    public void connectionAdded(final Connection connection) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        registrationExecutor.submit(() -> {
            for (ClientListenerRegistration listenerRegistration : registrations.values()) {
                invokeFromInternalThread(listenerRegistration, connection);
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
    public void connectionRemoved(final Connection connection) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        registrationExecutor.submit(() -> {
            for (ClientListenerRegistration registry : registrations.values()) {
                registry.getConnectionRegistrations().remove(connection);
            }
        });
    }

    //called from ee.
    public StripedExecutor getEventExecutor() {
        return eventExecutor;
    }

    //For Testing
    public Map<Connection, ClientConnectionRegistration> getActiveRegistrations(final UUID uuid) {
        //This method should not be called from registrationExecutor
        assert (!Thread.currentThread().getName().contains("eventRegistration"));

        Future<Map<Connection, ClientConnectionRegistration>> future = registrationExecutor.submit(() -> {
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

    private void invokeFromInternalThread(ClientListenerRegistration registrationKey, Connection connection) {
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
        return isSmart;
    }

    private Boolean deregisterListenerInternal(@Nullable UUID userRegistrationId) {
        //This method should only be called from registrationExecutor
        assert (Thread.currentThread().getName().contains("eventRegistration"));

        ClientListenerRegistration listenerRegistration = registrations.remove(userRegistrationId);
        if (listenerRegistration == null) {
            return false;
        }

        Map<Connection, ClientConnectionRegistration> registrations = listenerRegistration.getConnectionRegistrations();
        CompletableFuture[] futures = new CompletableFuture[registrations.size()];
        int i = 0;
        for (Map.Entry<Connection, ClientConnectionRegistration> entry : registrations.entrySet()) {
            ClientConnectionRegistration registration = entry.getValue();
            ClientConnection subscriber = (ClientConnection) entry.getKey();
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
            futures[i++] = clientInvocation.invokeUrgent().exceptionally(throwable -> {
                if (!(throwable instanceof HazelcastClientNotActiveException
                        || throwable instanceof IOException
                        || throwable instanceof TargetDisconnectedException)) {
                    logger.warning("Deregistration of listener with ID " + userRegistrationId
                            + " has failed for address " + subscriber.getRemoteAddress(), throwable);
                }
                return null;
            });
        }
        CompletableFuture.allOf(futures).join();
        return true;
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
