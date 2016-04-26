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
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;

public abstract class ClientListenerServiceImpl implements ClientListenerService {

    protected final HazelcastClientInstanceImpl client;
    protected final ClientExecutionServiceImpl executionService;
    protected final SerializationService serializationService;
    protected final ClientInvocationService invocationService;
    protected final ExecutorService registrationExecutor;
    protected final ILogger logger;
    private final ConcurrentMap<Long, EventHandler> eventHandlerMap
            = new ConcurrentHashMap<Long, EventHandler>();

    private final StripedExecutor eventExecutor;

    public ClientListenerServiceImpl(HazelcastClientInstanceImpl client, int eventThreadCount, int eventQueueCapacity) {
        this.client = client;
        executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        invocationService = client.getInvocationService();
        serializationService = client.getSerializationService();
        logger = client.getLoggingService().getLogger(ClientListenerService.class);
        ThreadGroup threadGroup = client.getThreadGroup();
        String name = client.getName();
        eventExecutor = new StripedExecutor(logger, name + ".event",
                threadGroup, eventThreadCount, eventQueueCapacity);
        ClassLoader classLoader = client.getClientConfig().getClassLoader();

        ThreadFactory threadFactory = new SingleExecutorThreadFactory(threadGroup, classLoader, name + ".eventRegistration-");
        registrationExecutor = Executors.newSingleThreadExecutor(threadFactory);
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
            logger.log(Level.WARNING, " event clientMessage could not be handled ", e);
        }
    }

    public void shutdown() {
        eventExecutor.shutdown();
        ClientExecutionServiceImpl.shutdownExecutor("registrationExecutor", registrationExecutor, logger);
    }

    public StripedExecutor getEventExecutor() {
        return eventExecutor;
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

    //For Testing
    public abstract Collection<ClientEventRegistration> getActiveRegistrations(String uuid);

}
