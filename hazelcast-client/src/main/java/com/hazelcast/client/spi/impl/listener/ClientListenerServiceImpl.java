/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;

public abstract class ClientListenerServiceImpl implements ClientListenerService, MembershipListener {

    protected final HazelcastClientInstanceImpl client;
    protected final ClientExecutionServiceImpl executionService;
    protected final SerializationService serializationService;
    protected final ClientInvocationService invocationService;
    private final ConcurrentMap<Integer, EventHandler> eventHandlerMap
            = new ConcurrentHashMap<Integer, EventHandler>();
    private final ILogger logger = Logger.getLogger(ClientInvocationService.class);

    private final StripedExecutor eventExecutor;

    public ClientListenerServiceImpl(HazelcastClientInstanceImpl client, int eventThreadCount, int eventQueueCapacity) {
        this.client = client;
        this.executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        this.invocationService = client.getInvocationService();
        this.serializationService = client.getSerializationService();
        client.getClientClusterService().addMembershipListener(this);
        this.eventExecutor = new StripedExecutor(logger, client.getName() + ".event",
                client.getThreadGroup(), eventThreadCount, eventQueueCapacity);
    }

    public void addEventHandler(int callId, EventHandler handler) {
        eventHandlerMap.put(callId, handler);
    }

    protected void removeEventHandler(int callId) {
        eventHandlerMap.remove(callId);
    }

    public void handleClientMessage(ClientMessage clientMessage) {
        try {
            eventExecutor.execute(new ClientEventProcessor(clientMessage));
        } catch (RejectedExecutionException e) {
            logger.log(Level.WARNING, " event clientMessage could not be handled ", e);
        }
    }

    public void shutdown() {
        eventExecutor.shutdown();
    }

    public StripedExecutor getEventExecutor() {
        return eventExecutor;
    }

    private final class ClientEventProcessor implements StripedRunnable {
        final ClientMessage clientMessage;

        private ClientEventProcessor(ClientMessage clientMessage) {
            this.clientMessage = clientMessage;
        }

        @Override
        public void run() {
            int correlationId = clientMessage.getCorrelationId();
            final EventHandler eventHandler = eventHandlerMap.get(correlationId);
            if (eventHandler == null) {
                logger.warning("No eventHandler for callId: " + correlationId + ", event: " + clientMessage);
                return;
            }

            eventHandler.handle(clientMessage);
        }

        @Override
        public int getKey() {
            return clientMessage.getPartitionId();
        }
    }
}
