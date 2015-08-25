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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.impl.client.ClientResponse;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;

public final class ClientListenerServiceImpl implements ClientListenerService {

    private final ILogger logger = Logger.getLogger(ClientInvocationService.class);
    private final HazelcastClientInstanceImpl client;
    private final ClientInvocationService invocationService;
    private final InternalSerializationService serializationService;
    private final ConcurrentMap<String, Integer> registrationMap = new ConcurrentHashMap<String, Integer>();
    private final ConcurrentMap<String, String> registrationAliasMap = new ConcurrentHashMap<String, String>();
    private final StripedExecutor eventExecutor;

    private final Set<ClientInvocation> failedListeners =
            Collections.newSetFromMap(new ConcurrentHashMap<ClientInvocation, Boolean>());

    public ClientListenerServiceImpl(HazelcastClientInstanceImpl client, int eventThreadCount, int eventQueueCapacity) {
        this.client = client;
        this.invocationService = client.getInvocationService();
        this.serializationService = client.getSerializationService();
        this.eventExecutor = new StripedExecutor(logger, client.getName() + ".event",
                client.getThreadGroup(), eventThreadCount, eventQueueCapacity);
    }

    public StripedExecutor getEventExecutor() {
        return eventExecutor;
    }

    @Override
    public String startListening(ClientRequest request, Object key, EventHandler handler) {
        final Future future;
        try {
            handler.beforeListenerRegister();

            if (key == null) {
                future = new ClientInvocation(client, handler, request).invoke();
            } else {
                final int partitionId = client.getClientPartitionService().getPartitionId(key);
                future = new ClientInvocation(client, handler, request, partitionId).invoke();
            }
            String registrationId = serializationService.toObject(future.get());
            registerListener(registrationId, request.getCallId());
            return registrationId;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean stopListening(BaseClientRemoveListenerRequest request, String registrationId) {
        try {
            String realRegistrationId = deRegisterListener(registrationId);
            if (realRegistrationId == null) {
                return false;
            }
            request.setRegistrationId(realRegistrationId);
            final Future<Boolean> future = new ClientInvocation(client, request).invoke();
            return (Boolean) serializationService.toObject(future.get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public void registerFailedListener(ClientInvocation future) {
        failedListeners.add(future);
    }

    public void triggerFailedListeners() {
        final Iterator<ClientInvocation> iterator = failedListeners.iterator();
        while (iterator.hasNext()) {
            final ClientInvocation failedListener = iterator.next();
            iterator.remove();
            failedListener.notify(new TargetDisconnectedException());
        }
    }

    public void registerListener(String uuid, Integer callId) {
        registrationAliasMap.put(uuid, uuid);
        registrationMap.put(uuid, callId);
    }

    public void reRegisterListener(String uuid, String alias, Integer callId) {
        final String oldAlias = registrationAliasMap.put(uuid, alias);
        if (oldAlias != null) {
            registrationMap.remove(oldAlias);
            registrationMap.put(alias, callId);
        }
    }

    public String deRegisterListener(String alias) {
        final String uuid = registrationAliasMap.remove(alias);
        if (uuid != null) {
            final Integer callId = registrationMap.remove(alias);
            invocationService.removeEventHandler(callId);
        }
        return uuid;
    }

    public void handleEventPacket(Packet packet) {
        try {
            eventExecutor.execute(new ClientEventProcessor(packet));
        } catch (RejectedExecutionException e) {
            logger.log(Level.WARNING, " event packet could not be handled ", e);
        }
    }

    public void shutdown() {
        eventExecutor.shutdown();
    }

    private final class ClientEventProcessor implements StripedRunnable {
        final Packet packet;

        private ClientEventProcessor(Packet packet) {
            this.packet = packet;
        }

        @Override
        public void run() {
            final ClientConnection conn = (ClientConnection) packet.getConn();
            final ClientResponse clientResponse = serializationService.toObject(packet);
            final int callId = clientResponse.getCallId();
            final Data response = clientResponse.getResponse();
            handleEvent(response, callId, conn);
        }

        private void handleEvent(Data event, int callId, ClientConnection conn) {
            final Object eventObject = serializationService.toObject(event);
            final EventHandler eventHandler = invocationService.getEventHandler(callId);
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
}
