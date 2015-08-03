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

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
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
    private final ConcurrentMap<String, Integer> registrationMap = new ConcurrentHashMap<String, Integer>();
    private final ConcurrentMap<String, String> registrationAliasMap = new ConcurrentHashMap<String, String>();
    private final StripedExecutor eventExecutor;

    private final Set<ClientInvocation> failedListeners =
            Collections.newSetFromMap(new ConcurrentHashMap<ClientInvocation, Boolean>());

    public ClientListenerServiceImpl(HazelcastClientInstanceImpl client, int eventThreadCount, int eventQueueCapacity) {
        this.client = client;
        this.invocationService = client.getInvocationService();
        this.eventExecutor = new StripedExecutor(logger, client.getName() + ".event",
                client.getThreadGroup(), eventThreadCount, eventQueueCapacity);
    }

    public StripedExecutor getEventExecutor() {
        return eventExecutor;
    }

    @Override
    public String startListening(ClientMessage clientMessage, Object key, EventHandler handler,
                                 ClientMessageDecoder responseDecoder) {
        final ClientInvocationFuture future;
        try {
            handler.beforeListenerRegister();

            if (key == null) {
                future = new ClientListenerInvocation(client, handler, clientMessage, responseDecoder).invoke();
            } else {
                final int partitionId = client.getClientPartitionService().getPartitionId(key);
                future = new ClientListenerInvocation(client, handler, clientMessage, partitionId,
                        responseDecoder).invoke();
            }
            ClientMessage responseMessage = future.get();
            String registrationId = responseDecoder.decodeClientMessage(responseMessage);

            registerListener(registrationId, clientMessage.getCorrelationId());
            return registrationId;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean stopListening(String registrationId, ListenerRemoveCodec listenerRemoveCodec) {
        try {
            String realRegistrationId = deRegisterListener(registrationId);
            if (realRegistrationId == null) {
                return false;
            }
            ClientMessage removeRequest = listenerRemoveCodec.encodeRequest(realRegistrationId);
            final Future future = new ClientInvocation(client, removeRequest).invoke();
            return listenerRemoveCodec.decodeResponse((ClientMessage) future.get());
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
            failedListener.notifyException(new TargetDisconnectedException());
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
            final Integer callId = registrationMap.remove(uuid);
            invocationService.removeEventHandler(callId);
        }
        return uuid;
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

    private final class ClientEventProcessor implements StripedRunnable {
        final ClientMessage clientMessage;

        private ClientEventProcessor(ClientMessage clientMessage) {
            this.clientMessage = clientMessage;
        }

        @Override
        public void run() {
            int correlationId = clientMessage.getCorrelationId();
            final EventHandler eventHandler = invocationService.getEventHandler(correlationId);
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
