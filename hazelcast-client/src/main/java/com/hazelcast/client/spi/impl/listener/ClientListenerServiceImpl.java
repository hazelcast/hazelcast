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

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;

public abstract class ClientListenerServiceImpl implements ClientListenerService, MetricsProvider {

    protected final HazelcastClientInstanceImpl client;
    protected final SerializationService serializationService;
    protected final ScheduledExecutorService registrationExecutor;
    protected final ILogger logger;

    @Probe(name = "eventHandlerCount", level = MANDATORY)
    private final ConcurrentMap<Long, EventHandler> eventHandlerMap
            = new ConcurrentHashMap<Long, EventHandler>();

    private final StripedExecutor eventExecutor;

    public ClientListenerServiceImpl(HazelcastClientInstanceImpl client, int eventThreadCount, int eventQueueCapacity) {
        this.client = client;
        serializationService = client.getSerializationService();
        logger = client.getLoggingService().getLogger(ClientListenerService.class);
        String name = client.getName();
        eventExecutor = new StripedExecutor(logger, name + ".event", eventThreadCount, eventQueueCapacity);
        ClassLoader classLoader = client.getClientConfig().getClassLoader();

        ThreadFactory threadFactory = new SingleExecutorThreadFactory(classLoader, name + ".eventRegistration-");
        registrationExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    @Override
    public void provideMetrics(MetricsRegistry registry) {
        registry.scanAndRegister(this, "listeners");
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

    protected void removeEventHandler(long callId) {
        eventHandlerMap.remove(callId);
    }

    protected EventHandler getEventHandler(long callId) {
        return eventHandlerMap.get(callId);
    }

    public void handleClientMessage(ClientMessage clientMessage, Connection connection) {
        try {
            eventExecutor.execute(new ClientEventProcessor(clientMessage, (ClientConnection) connection));
        } catch (RejectedExecutionException e) {
            logger.warning("Event clientMessage could not be handled", e);
        }
    }

    public void shutdown() {
        eventExecutor.shutdown();
        ClientExecutionServiceImpl.shutdownExecutor("registrationExecutor", registrationExecutor, logger);
    }

    public void start() {
    }

    private final class ClientEventProcessor implements StripedRunnable {
        final ClientMessage clientMessage;
        final ClientConnection connection;

        private ClientEventProcessor(ClientMessage clientMessage, ClientConnection connection) {
            this.clientMessage = clientMessage;
            this.connection = connection;
        }

        @Override
        public void run() {
            try {
                long correlationId = clientMessage.getCorrelationId();
                final EventHandler eventHandler = eventHandlerMap.get(correlationId);
                if (eventHandler == null) {
                    logger.warning("No eventHandler for callId: " + correlationId + ", event: " + clientMessage
                            + ", connection: " + connection);
                    return;
                }

                eventHandler.handle(clientMessage);
            } finally {
                connection.decrementPendingPacketCount();
            }
        }

        @Override
        public int getKey() {
            return clientMessage.getPartitionId();
        }
    }

    //called from ee.
    public StripedExecutor getEventExecutor() {
        return eventExecutor;
    }

    //For Testing
    public abstract Collection<ClientEventRegistration> getActiveRegistrations(String uuid);

}
