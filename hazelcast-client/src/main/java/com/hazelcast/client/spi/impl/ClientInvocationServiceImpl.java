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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ErrorCodec;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.spi.properties.ClientProperty.INVOCATION_TIMEOUT_SECONDS;
import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;
import static com.hazelcast.spi.impl.operationservice.impl.AsyncInboundResponseHandler.getIdleStrategy;

public abstract class ClientInvocationServiceImpl implements ClientInvocationService {

    private static final HazelcastProperty IDLE_STRATEGY
            = new HazelcastProperty("hazelcast.client.responsequeue.idlestrategy", "block");

    private static final int WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED_THRESHOLD = 5000;

    protected final HazelcastClientInstanceImpl client;
    protected final ILogger invocationLogger;

    protected ClientConnectionManager connectionManager;
    protected ClientPartitionService partitionService;
    private ClientListenerServiceImpl clientListenerService;

    @Probe(name = "pendingCalls", level = ProbeLevel.MANDATORY)
    private ConcurrentMap<Long, ClientInvocation> callIdMap = new ConcurrentHashMap<Long, ClientInvocation>();

    private ResponseThread responseThread;

    private volatile boolean isShutdown;
    private final long invocationTimeoutMillis;

    public ClientInvocationServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.invocationLogger = client.getLoggingService().getLogger(ClientInvocationService.class);
        this.invocationTimeoutMillis = initInvocationTimeoutMillis();
        client.getMetricsRegistry().scanAndRegister(this, "invocations");
    }

    private long initInvocationTimeoutMillis() {
        long waitTime = client.getProperties().getMillis(INVOCATION_TIMEOUT_SECONDS);
        return waitTime > 0 ? waitTime : Integer.parseInt(INVOCATION_TIMEOUT_SECONDS.getDefaultValue());
    }

    public void start() {
        connectionManager = client.getConnectionManager();
        clientListenerService = (ClientListenerServiceImpl) client.getListenerService();
        partitionService = client.getClientPartitionService();
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        responseThread = new ResponseThread(client.getName() + ".response-", classLoader);
        responseThread.start();
        ClientExecutionService executionService = client.getClientExecutionService();
        executionService.scheduleWithRepetition(new CleanResourcesTask(), 1, 1, TimeUnit.SECONDS);
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
                throw new IOException("Packet not send to " + connection.getEndPoint());
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
                invocationLogger.warning("Connection is not heart-beating, won't write client message -> "
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
        callIdMap.put(correlationId, clientInvocation);
        EventHandler handler = clientInvocation.getEventHandler();
        if (handler != null) {
            clientListenerService.addEventHandler(correlationId, handler);
        }
    }

    private ClientInvocation deRegisterCallId(long callId) {
        return callIdMap.remove(callId);
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    public void shutdown() {
        isShutdown = true;
        responseThread.interrupt();
        Iterator<ClientInvocation> iterator = callIdMap.values().iterator();
        while (iterator.hasNext()) {
            ClientInvocation invocation = iterator.next();
            iterator.remove();
            invocation.notifyException(new HazelcastClientNotActiveException("Client is shutting down"));
        }
        assert callIdMap.isEmpty();
    }

    private class CleanResourcesTask implements Runnable {

        @Override
        public void run() {
            Iterator<Map.Entry<Long, ClientInvocation>> iter = callIdMap.entrySet().iterator();
            Collection<ClientConnection> expiredConnections = null;
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

                if (connection.getPendingPacketCount() != 0) {
                    long closedTime = connection.getClosedTime();
                    long elapsed = System.currentTimeMillis() - closedTime;
                    if (elapsed < WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED_THRESHOLD) {
                        continue;
                    } else {
                        if (expiredConnections == null) {
                            expiredConnections = new LinkedList<ClientConnection>();
                        }
                        expiredConnections.add(connection);
                    }
                }

                iter.remove();

                notifyException(invocation, connection);
            }
            if (expiredConnections != null) {
                logExpiredConnections(expiredConnections);
            }
        }

        private void notifyException(ClientInvocation invocation, ClientConnection connection) {
            Exception ex;
            /**
             * Connection may be closed(e.g. remote member shutdown) in which case the isAlive is set to false or the
             * heartbeat failure occurs. The order of the following check matters. We need to first check for isAlive since
             * the connection.isHeartBeating also checks for isAlive as well.
             */
            if (!connection.isAlive()) {
                ex = new TargetDisconnectedException(connection.getCloseReason(), connection.getCloseCause());
            } else {
                ex = new TargetDisconnectedException("Heartbeat timed out to " + connection);
            }

            invocation.notifyException(ex);
        }

        private void logExpiredConnections(Collection<ClientConnection> expiredConnections) {
            for (ClientConnection expiredConnection : expiredConnections) {
                int pendingPacketCount = expiredConnection.getPendingPacketCount();
                if (pendingPacketCount != 0) {
                    invocationLogger.warning("There are " + pendingPacketCount
                            + " packets which are not processed on "
                            + expiredConnection.getEndPoint());
                }
            }
        }
    }

    @Override
    public void handleClientMessage(ClientMessage message, Connection connection) {
        responseThread.responseQueue.add(new ClientPacket((ClientConnection) connection, message));
    }

    public long getInvocationTimeoutMillis() {
        return invocationTimeoutMillis;
    }

    private static class ClientPacket {

        private final ClientConnection clientConnection;
        private final ClientMessage clientMessage;

        ClientPacket(ClientConnection clientConnection, ClientMessage clientMessage) {
            this.clientConnection = clientConnection;
            this.clientMessage = clientMessage;
        }

        private ClientConnection getClientConnection() {
            return clientConnection;
        }

        private ClientMessage getClientMessage() {
            return clientMessage;
        }
    }

    private class ResponseThread extends Thread {

        private final BlockingQueue<ClientPacket> responseQueue;

        ResponseThread(String name, ClassLoader classLoader) {
            super(name);
            setContextClassLoader(classLoader);

            this.responseQueue = new MPSCQueue<ClientPacket>(this, getIdleStrategy(client.getProperties(), IDLE_STRATEGY));
        }

        @Override
        public void run() {
            try {
                doRun();
            } catch (OutOfMemoryError e) {
                onOutOfMemory(e);
            } catch (Throwable t) {
                invocationLogger.severe(t);
            }
        }

        private void doRun() {
            while (!isShutdown) {
                ClientPacket task;
                try {
                    task = responseQueue.take();
                } catch (InterruptedException e) {
                    continue;
                }
                process(task);
            }
        }

        private void process(ClientPacket packet) {
            final ClientConnection conn = packet.getClientConnection();
            try {
                handleClientMessage(packet.getClientMessage());
            } catch (Exception e) {
                invocationLogger.severe("Failed to process task: " + packet + " on responseThread: " + getName(), e);
            } finally {
                conn.decrementPendingPacketCount();
            }
        }

        private void handleClientMessage(ClientMessage clientMessage) {
            long correlationId = clientMessage.getCorrelationId();

            final ClientInvocation future = deRegisterCallId(correlationId);
            if (future == null) {
                invocationLogger.warning("No call for callId: " + correlationId + ", response: " + clientMessage);
                return;
            }
            if (ErrorCodec.TYPE == clientMessage.getMessageType()) {
                Throwable exception = client.getClientExceptionFactory().createException(clientMessage);
                future.notifyException(exception);
            } else {
                future.notify(clientMessage);
            }
        }
    }
}
