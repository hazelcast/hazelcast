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

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientResponse;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;

public abstract class ClientListenerServiceImpl implements ClientListenerService {

    protected final HazelcastClientInstanceImpl client;
    protected final ClientExecutionServiceImpl executionService;
    protected final SerializationService serializationService;
    protected final ClientInvocationService invocationService;
    protected final ILogger logger = Logger.getLogger(ClientListenerService.class);
    private final ConcurrentMap<Integer, EventHandler> eventHandlerMap
            = new ConcurrentHashMap<Integer, EventHandler>();

    private final StripedExecutor eventExecutor;

    public ClientListenerServiceImpl(HazelcastClientInstanceImpl client, int eventThreadCount, int eventQueueCapacity) {
        this.client = client;
        this.executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        this.invocationService = client.getInvocationService();
        this.serializationService = client.getSerializationService();
        this.eventExecutor = new StripedExecutor(logger, client.getName() + ".event",
                client.getThreadGroup(), eventThreadCount, eventQueueCapacity);
    }


    public void addEventHandler(int callId, EventHandler handler) {
        eventHandlerMap.put(callId, handler);
    }

    protected void removeEventHandler(int callId) {
        eventHandlerMap.remove(callId);
    }

    protected EventHandler getEventHandler(int callId) {
        return eventHandlerMap.get(callId);
    }

    public void handleEventPacket(Packet packet) {
        try {
            eventExecutor.execute(new ClientEventProcessor(packet));
        } catch (RejectedExecutionException e) {
            logger.log(Level.WARNING, " event packet could not be handled ", e);
        }
    }

    public void start() {
    }

    public void shutdown() {
        eventExecutor.shutdown();
    }

    public StripedExecutor getEventExecutor() {
        return eventExecutor;
    }

    private final class ClientEventProcessor implements StripedRunnable {
        private final Packet packet;

        private ClientEventProcessor(Packet packet) {
            this.packet = packet;
        }

        @Override
        public void run() {
            ClientConnection conn = (ClientConnection) packet.getConn();
            try {
                ClientResponse clientResponse = serializationService.toObject(packet);
                int callId = clientResponse.getCallId();
                Data response = clientResponse.getResponse();
                handleEvent(response, callId, conn);
            } finally {
                conn.decrementPendingPacketCount();
            }
        }

        private void handleEvent(Data event, int callId, ClientConnection conn) {
            Object eventObject = serializationService.toObject(event);
            EventHandler eventHandler = eventHandlerMap.get(callId);
            if (eventHandler == null) {
                logger.warning("No eventHandler for callId: " + callId + ", event: " + eventObject + ", conn: " + conn);
                return;
            }
            eventHandler.handle(eventObject);
        }

        @Override
        public int getKey() {
            return packet.getPartitionId();
        }
    }

    //For Testing
    public abstract Collection<ClientEventRegistration> getActiveRegistrations(String uuid);
}
