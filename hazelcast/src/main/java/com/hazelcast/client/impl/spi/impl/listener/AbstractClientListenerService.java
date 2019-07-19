/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.connection.nio.ClientConnection;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.AbstractClientInvocationService;
import com.hazelcast.client.impl.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.impl.spi.ClientListenerService;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.executor.SingleExecutorThreadFactory;
import com.hazelcast.internal.util.executor.StripedExecutor;
import com.hazelcast.internal.util.executor.StripedRunnable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public abstract class AbstractClientListenerService implements ClientListenerService, StaticMetricsProvider, ConnectionListener {

    protected final HazelcastClientInstanceImpl client;
    protected final SerializationService serializationService;
    protected final long invocationTimeoutMillis;
    protected final long invocationRetryPauseMillis;
    protected final Map<UUID, ClientListenerRegistration> registrations = new ConcurrentHashMap<>();

    final ScheduledExecutorService registrationExecutor;
    final ClientConnectionManager clientConnectionManager;

    private final ILogger logger;

    @Probe(name = "eventHandlerCount", level = MANDATORY)
    private final ConcurrentMap<Long, EventHandler> eventHandlerMap
            = new ConcurrentHashMap<Long, EventHandler>();

    private final StripedExecutor eventExecutor;

    AbstractClientListenerService(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.serializationService = client.getSerializationService();
        this.logger = client.getLoggingService().getLogger(ClientListenerService.class);
        String name = client.getName();
        HazelcastProperties properties = client.getProperties();
        int eventQueueCapacity = properties.getInteger(ClientProperty.EVENT_QUEUE_CAPACITY);
        int eventThreadCount = properties.getInteger(ClientProperty.EVENT_THREAD_COUNT);
        this.eventExecutor = new StripedExecutor(logger, name + ".event", eventThreadCount, eventQueueCapacity, true);
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        ThreadFactory threadFactory = new SingleExecutorThreadFactory(classLoader, name + ".eventRegistration-");
        this.registrationExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
        this.clientConnectionManager = client.getConnectionManager();
        AbstractClientInvocationService invocationService = (AbstractClientInvocationService) client.getInvocationService();
        this.invocationTimeoutMillis = invocationService.getInvocationTimeoutMillis();
        this.invocationRetryPauseMillis = invocationService.getInvocationRetryPauseMillis();
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
            for (ClientConnection connection : connections) {
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
        registry.registerStaticMetrics(this, "listeners");
    }

    @Probe(level = MANDATORY)
    private int eventQueueSize() {
        return eventExecutor.getWorkQueueSize();
    }

    @Probe(level = MANDATORY)
    private long eventsProcessed() {
        return eventExecutor.processedCount();
    }

    public void addEventHandler(long callId, EventHandler handler) {
        eventHandlerMap.put(callId, handler);
    }

    public void handleEventMessage(ClientMessage clientMessage) {
        try {
            eventExecutor.execute(new ClientEventProcessor(clientMessage));
        } catch (RejectedExecutionException e) {
            logger.warning("Event clientMessage could not be handled", e);
        }
    }

    public void handleEventMessageOnCallingThread(ClientMessage clientMessage) {
        long correlationId = clientMessage.getCorrelationId();
        EventHandler eventHandler = eventHandlerMap.get(correlationId);
        if (eventHandler == null) {
            logger.warning("No eventHandler for callId: " + correlationId + ", event: " + clientMessage);
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

        UUID serverRegistrationId = codec.decodeAddResponse(clientMessage);
        handler.onListenerRegister();
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
        ClientExecutionServiceImpl.shutdownExecutor("registrationExecutor", registrationExecutor, logger);
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
                ClientConnectionRegistration registration = registry.getConnectionRegistrations().remove(connection);
                if (registration != null) {
                    removeEventHandler(registration.getCallId());
                }
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

    // used in tests
    public Map<Long, EventHandler> getEventHandlers() {
        return Collections.unmodifiableMap(eventHandlerMap);
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

    abstract boolean registersLocalOnly();

    public void removeEventHandler(long callId) {
        eventHandlerMap.remove(callId);
    }

    private Boolean deregisterListenerInternal(@Nullable UUID userRegistrationId) {
        //This method should only be called from registrationExecutor
        assert (Thread.currentThread().getName().contains("eventRegistration"));

        ClientListenerRegistration listenerRegistration = registrations.get(userRegistrationId);
        if (listenerRegistration == null) {
            return false;
        }
        boolean successful = true;

        Map<Connection, ClientConnectionRegistration> registrations = listenerRegistration.getConnectionRegistrations();
        Iterator<Map.Entry<Connection, ClientConnectionRegistration>> iterator = registrations.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Connection, ClientConnectionRegistration> entry = iterator.next();
            ClientConnectionRegistration registration = entry.getValue();
            Connection subscriber = entry.getKey();
            try {
                ListenerMessageCodec listenerMessageCodec = listenerRegistration.getCodec();
                UUID serverRegistrationId = registration.getServerRegistrationId();
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
            this.registrations.remove(userRegistrationId);
        }
        return successful;
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
