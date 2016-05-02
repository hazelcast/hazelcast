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

package com.hazelcast.client.spi.impl;

import com.hazelcast.cache.impl.JCacheDetector;
import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ErrorCodec;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.exception.TargetDisconnectedException;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.spi.properties.ClientProperty.MAX_CONCURRENT_INVOCATIONS;
import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;
import static com.hazelcast.util.StringUtil.timeToString;
import static java.lang.String.format;


abstract class ClientInvocationServiceSupport implements ClientInvocationService {

    private static final int WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED_THRESHOLD = 5000;
    protected final HazelcastClientInstanceImpl client;
    protected ClientConnectionManager connectionManager;
    protected ClientPartitionService partitionService;
    protected ClientExecutionService executionService;
    protected ClientListenerServiceImpl clientListenerService;
    protected final ILogger invocationLogger;
    private ResponseThread responseThread;

    @Probe(name = "pendingCalls", level = ProbeLevel.MANDATORY)
    private ConcurrentMap<Long, ClientInvocation> callIdMap
            = new ConcurrentHashMap<Long, ClientInvocation>();

    private final CallIdSequence callIdSequence;
    private ClientExceptionFactory clientExceptionFactory;
    private volatile boolean isShutdown;

    public ClientInvocationServiceSupport(HazelcastClientInstanceImpl client) {
        this.client = client;
        int maxAllowedConcurrentInvocations = client.getProperties().getInteger(MAX_CONCURRENT_INVOCATIONS);
        callIdSequence = new CallIdSequence.CallIdSequenceFailFast(maxAllowedConcurrentInvocations);
        invocationLogger = client.getLoggingService().getLogger(ClientInvocationService.class);

        client.getMetricsRegistry().scanAndRegister(this, "invocations");
    }

    @Override
    public void start() {
        connectionManager = client.getConnectionManager();
        executionService = client.getClientExecutionService();
        clientListenerService = (ClientListenerServiceImpl) client.getListenerService();
        partitionService = client.getClientPartitionService();
        clientExceptionFactory = initClientExceptionFactory();
        responseThread = new ResponseThread(client.getThreadGroup(), client.getName() + ".response-",
                client.getClientConfig().getClassLoader());
        responseThread.start();
        executionService.scheduleWithRepetition(new CleanResourcesTask(), 1, 1, TimeUnit.SECONDS);
    }


    private ClientExceptionFactory initClientExceptionFactory() {
        boolean jcacheAvailable = JCacheDetector.isJcacheAvailable(client.getClientConfig().getClassLoader());
        return new ClientExceptionFactory(jcacheAvailable);
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
                callIdSequence.complete();
                throw new IOException("Packet not send to " + connection.getRemoteEndpoint());
            } else {
                if (invocationLogger.isFinestEnabled()) {
                    invocationLogger.finest("Invocation not found to deregister for call id " + callId);
                }
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
                //ping and removeAllListeners should be send even though heart is not beating
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
        long correlationId;
        if (clientInvocation.isUrgent()) {
            correlationId = callIdSequence.renew();
        } else {
            correlationId = callIdSequence.next();
        }
        clientInvocation.getClientMessage().setCorrelationId(correlationId).setVersion(protocolVersion);
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

                Exception ex = new TargetDisconnectedException(format(
                        "Disconnecting from member %s due to heartbeat problems. Current time: %s. Last heartbeat: %s",
                        connection.getRemoteEndpoint(),
                        timeToString(System.currentTimeMillis()),
                        timeToString(connection.getLastHeartbeatMillis())), connection.getCloseCause());
                invocation.notifyException(ex);
            }
            if (expiredConnections != null) {
                logExpiredConnections(expiredConnections);
            }
        }

        private void logExpiredConnections(Collection<ClientConnection> expiredConnections) {
            for (ClientConnection expiredConnection : expiredConnections) {
                int pendingPacketCount = expiredConnection.getPendingPacketCount();
                if (pendingPacketCount != 0) {
                    invocationLogger.warning("There are " + pendingPacketCount
                            + " packets which are not processed "
                            + " on " + expiredConnection.getRemoteEndpoint());
                }
            }
        }
    }

    @Override
    public void handleClientMessage(ClientMessage message, Connection connection) {
        responseThread.workQueue.add(new ClientPacket((ClientConnection) connection, message));
    }

    private static class ClientPacket {
        private final ClientConnection clientConnection;
        private final ClientMessage clientMessage;

        public ClientPacket(ClientConnection clientConnection, ClientMessage clientMessage) {
            this.clientConnection = clientConnection;
            this.clientMessage = clientMessage;
        }

        public ClientConnection getClientConnection() {
            return clientConnection;
        }

        public ClientMessage getClientMessage() {
            return clientMessage;
        }
    }

    private class ResponseThread extends Thread {
        private final BlockingQueue<ClientPacket> workQueue = new LinkedBlockingQueue<ClientPacket>();

        public ResponseThread(ThreadGroup threadGroup, String name, ClassLoader classLoader) {
            super(threadGroup, name);
            setContextClassLoader(classLoader);
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
            while (true) {
                ClientPacket task;
                try {
                    task = workQueue.take();
                } catch (InterruptedException e) {
                    if (isShutdown) {
                        return;
                    }
                    continue;
                }

                if (isShutdown) {
                    return;
                }
                process(task);
            }
        }

        private void process(ClientPacket packet) {
            final ClientConnection conn = packet.getClientConnection();
            try {
                handleClientMessage(packet.getClientMessage());
            } catch (Exception e) {
                invocationLogger.severe("Failed to process task: " + packet + " on responseThread :" + getName(), e);
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
            callIdSequence.complete();
            if (ErrorCodec.TYPE == clientMessage.getMessageType()) {
                Throwable exception = clientExceptionFactory.createException(clientMessage);
                future.notifyException(exception);
            } else {
                future.notify(clientMessage);
            }
        }

    }


}
