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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.listener.AbstractClientListenerService;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.client.spi.properties.ClientProperty.INVOCATION_RETRY_PAUSE_MILLIS;
import static com.hazelcast.client.spi.properties.ClientProperty.INVOCATION_TIMEOUT_SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class AbstractClientInvocationService implements ClientInvocationService {

    private static final HazelcastProperty CLEAN_RESOURCES_MILLIS
            = new HazelcastProperty("hazelcast.client.internal.clean.resources.millis", 100, MILLISECONDS);

    protected final HazelcastClientInstanceImpl client;

    protected ClientConnectionManager connectionManager;
    protected ClientPartitionService partitionService;
    final ILogger invocationLogger;
    private AbstractClientListenerService clientListenerService;

    @Probe(name = "pendingCalls", level = ProbeLevel.MANDATORY)
    private ConcurrentMap<Long, ClientInvocation> invocations = new ConcurrentHashMap<Long, ClientInvocation>();

    private ClientResponseHandlerSupplier responseHandlerSupplier;

    private volatile boolean isShutdown;
    private final long invocationTimeoutMillis;
    private final long invocationRetryPauseMillis;

    public AbstractClientInvocationService(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.invocationLogger = client.getLoggingService().getLogger(ClientInvocationService.class);
        this.invocationTimeoutMillis = initInvocationTimeoutMillis();
        this.invocationRetryPauseMillis = initInvocationRetryPauseMillis();
        this.responseHandlerSupplier = new ClientResponseHandlerSupplier(this);
        client.getMetricsRegistry().scanAndRegister(this, "invocations");
    }

    private long initInvocationRetryPauseMillis() {
        long pauseTime = client.getProperties().getMillis(INVOCATION_RETRY_PAUSE_MILLIS);
        return pauseTime > 0 ? pauseTime : Long.parseLong(INVOCATION_RETRY_PAUSE_MILLIS.getDefaultValue());
    }

    private long initInvocationTimeoutMillis() {
        long waitTime = client.getProperties().getMillis(INVOCATION_TIMEOUT_SECONDS);
        return waitTime > 0 ? waitTime : Integer.parseInt(INVOCATION_TIMEOUT_SECONDS.getDefaultValue());
    }

    public void start() {
        connectionManager = client.getConnectionManager();
        clientListenerService = (AbstractClientListenerService) client.getListenerService();
        partitionService = client.getClientPartitionService();
        responseHandlerSupplier.start();
        ClientExecutionService executionService = client.getClientExecutionService();
        long cleanResourcesMillis = client.getProperties().getMillis(CLEAN_RESOURCES_MILLIS);
        if (cleanResourcesMillis <= 0) {
            cleanResourcesMillis = Integer.parseInt(CLEAN_RESOURCES_MILLIS.getDefaultValue());

        }
        executionService.scheduleWithRepetition(new CleanResourcesTask(), cleanResourcesMillis,
                cleanResourcesMillis, MILLISECONDS);
    }

    @Override
    public ClientResponseHandler getResponseHandler() {
        return responseHandlerSupplier.get();
    }

    @Override
    public boolean isRedoOperation() {
        return client.getClientConfig().getNetworkConfig().isRedoOperation();
    }

    protected void send(ClientInvocation invocation, ClientConnection connection) throws IOException {
        if (isShutdown) {
            throw new HazelcastClientNotActiveException("Client is shut down");
        }
        registerInvocation(invocation);

        ClientMessage clientMessage = invocation.getClientMessage();
        if (!isAllowedToSendRequest(connection, invocation) || !writeToConnection(connection, clientMessage)) {
            final long callId = clientMessage.getCorrelationId();
            ClientInvocation clientInvocation = deRegisterCallId(callId);
            if (clientInvocation != null) {
                throw new IOException("Packet not sent to " + connection.getEndPoint());
            } else {
                if (invocationLogger.isFinestEnabled()) {
                    invocationLogger.finest("Invocation not found to deregister for call ID " + callId);
                }
                return;
            }
        }

        invocation.setSendConnection(connection);
    }

    private boolean writeToConnection(ClientConnection connection, ClientMessage clientMessage) {
        clientMessage.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
        return connection.write(clientMessage);
    }

    private boolean isAllowedToSendRequest(ClientConnection connection, ClientInvocation invocation) {
        if (!connection.isHeartBeating()) {
            if (invocation.shouldBypassHeartbeatCheck()) {
                // ping and removeAllListeners should be send even though heart is not beating
                return true;
            }

            if (invocationLogger.isFinestEnabled()) {
                invocationLogger.finest("Connection is not heart-beating, won't write client message -> "
                        + invocation.getClientMessage());
            }
            return false;
        }
        return true;
    }

    private void registerInvocation(ClientInvocation clientInvocation) {
        short protocolVersion = client.getProtocolVersion();

        ClientMessage clientMessage = clientInvocation.getClientMessage();
        clientMessage.setVersion(protocolVersion);
        long correlationId = clientMessage.getCorrelationId();
        invocations.put(correlationId, clientInvocation);
        EventHandler handler = clientInvocation.getEventHandler();
        if (handler != null) {
            clientListenerService.addEventHandler(correlationId, handler);
        }
    }

    ClientInvocation deRegisterCallId(long callId) {
        return invocations.remove(callId);
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    public void shutdown() {
        isShutdown = true;
        responseHandlerSupplier.shutdown();
        Iterator<ClientInvocation> iterator = invocations.values().iterator();
        while (iterator.hasNext()) {
            ClientInvocation invocation = iterator.next();
            iterator.remove();
            invocation.notifyException(new HazelcastClientNotActiveException("Client is shutting down"));
        }
    }

    public long getInvocationTimeoutMillis() {
        return invocationTimeoutMillis;
    }

    public long getInvocationRetryPauseMillis() {
        return invocationRetryPauseMillis;
    }

    private class CleanResourcesTask implements Runnable {

        @Override
        public void run() {
            Iterator<Map.Entry<Long, ClientInvocation>> iter = invocations.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, ClientInvocation> entry = iter.next();
                ClientInvocation invocation = entry.getValue();
                ClientConnection connection = invocation.getSendConnection();
                if (connection == null) {
                    continue;
                }

                if (connection.isHeartBeating()) {
                    continue;
                }

                iter.remove();

                notifyException(invocation, connection);
            }
        }

        private void notifyException(ClientInvocation invocation, ClientConnection connection) {
            Exception ex;

            // Connection may be closed(e.g. remote member shutdown) in which case the isAlive is set to false or the
            // heartbeat failure occurs. The order of the following check matters. We need to first check for isAlive since
            // the connection.isHeartBeating also checks for isAlive as well.
            if (!connection.isAlive()) {
                ex = new TargetDisconnectedException(connection.getCloseReason(), connection.getCloseCause());
            } else {
                ex = new TargetDisconnectedException("Heartbeat timed out to " + connection);
            }

            invocation.notifyException(ex);
        }
    }
}
