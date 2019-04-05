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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.listener.AbstractClientListenerService;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.impl.sequence.CallIdFactory;
import com.hazelcast.spi.impl.sequence.CallIdSequence;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.util.function.Consumer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.client.spi.properties.ClientProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS;
import static com.hazelcast.client.spi.properties.ClientProperty.INVOCATION_RETRY_PAUSE_MILLIS;
import static com.hazelcast.client.spi.properties.ClientProperty.INVOCATION_TIMEOUT_SECONDS;
import static com.hazelcast.client.spi.properties.ClientProperty.MAX_CONCURRENT_INVOCATIONS;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class AbstractClientInvocationService implements ClientInvocationService {

    private static final HazelcastProperty CLEAN_RESOURCES_MILLIS
            = new HazelcastProperty("hazelcast.client.internal.clean.resources.millis", 100, MILLISECONDS);

    protected final HazelcastClientInstanceImpl client;

    protected ClientConnectionManager connectionManager;
    protected ClientPartitionService partitionService;
    final ILogger invocationLogger;
    private AbstractClientListenerService clientListenerService;

    @Probe(name = "pendingCalls", level = MANDATORY)
    private ConcurrentMap<Long, ClientInvocation> invocations = new ConcurrentHashMap<Long, ClientInvocation>();

    private ClientResponseHandlerSupplier responseHandlerSupplier;

    private volatile boolean isShutdown;
    private final long invocationTimeoutMillis;
    private final long invocationRetryPauseMillis;
    private final CallIdSequence callIdSequence;

    public AbstractClientInvocationService(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.invocationLogger = client.getLoggingService().getLogger(ClientInvocationService.class);
        this.invocationTimeoutMillis = initInvocationTimeoutMillis();
        this.invocationRetryPauseMillis = initInvocationRetryPauseMillis();
        this.responseHandlerSupplier = new ClientResponseHandlerSupplier(this);

        HazelcastProperties properties = client.getProperties();
        int maxAllowedConcurrentInvocations = properties.getInteger(MAX_CONCURRENT_INVOCATIONS);
        long backofftimeoutMs = properties.getLong(BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS);
        // clients needs to have a call id generator capable of determining how many
        // pending calls there are. So backpressure needs to be on
        this.callIdSequence = CallIdFactory
                .newCallIdSequence(true, maxAllowedConcurrentInvocations, backofftimeoutMs);

        client.getMetricsRegistry().scanAndRegister(this, "invocations");
    }

    @Override
    public long concurrentInvocations() {
        return callIdSequence.concurrentInvocations();
    }

    private long initInvocationRetryPauseMillis() {
        return client.getProperties().getPositiveMillisOrDefault(INVOCATION_RETRY_PAUSE_MILLIS);

    }

    private long initInvocationTimeoutMillis() {
        return client.getProperties().getPositiveMillisOrDefault(INVOCATION_TIMEOUT_SECONDS);
    }

    @Probe(level = MANDATORY)
    private long startedInvocations() {
        return callIdSequence.getLastCallId();
    }

    @Probe(level = MANDATORY)
    private long maxCurrentInvocations() {
        return callIdSequence.getMaxConcurrentInvocations();
    }

    public long getInvocationTimeoutMillis() {
        return invocationTimeoutMillis;
    }

    public long getInvocationRetryPauseMillis() {
        return invocationRetryPauseMillis;
    }

    CallIdSequence getCallIdSequence() {
        return callIdSequence;
    }

    public void start() {
        connectionManager = client.getConnectionManager();
        clientListenerService = (AbstractClientListenerService) client.getListenerService();
        partitionService = client.getClientPartitionService();
        responseHandlerSupplier.start();
        ClientExecutionService executionService = client.getClientExecutionService();
        long cleanResourcesMillis = client.getProperties().getPositiveMillisOrDefault(CLEAN_RESOURCES_MILLIS);
        executionService.scheduleWithRepetition(new CleanResourcesTask(), cleanResourcesMillis,
                cleanResourcesMillis, MILLISECONDS);
    }

    @Override
    public Consumer<ClientMessage> getResponseHandler() {
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
        if (!writeToConnection(connection, clientMessage)) {
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

                if (connection.isAlive()) {
                    continue;
                }

                iter.remove();

                notifyException(invocation, connection);
            }
        }

        private void notifyException(ClientInvocation invocation, ClientConnection connection) {
            Exception ex = new TargetDisconnectedException(connection.getCloseReason(), connection.getCloseCause());
            invocation.notifyException(ex);
        }
    }
}
