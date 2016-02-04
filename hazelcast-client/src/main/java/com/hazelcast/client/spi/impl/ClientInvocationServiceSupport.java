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
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.internal.connection.Connection;
import com.hazelcast.internal.connection.ConnectionListener;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.util.ConstructorFunction;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;

import static com.hazelcast.client.config.ClientProperty.MAX_CONCURRENT_INVOCATIONS;
import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;


abstract class ClientInvocationServiceSupport implements ClientInvocationService, ConnectionListener
        , ConnectionHeartbeatListener {

    private static final int WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED = 10;
    private static final int WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED_THRESHOLD = 5000;
    protected final HazelcastClientInstanceImpl client;
    protected ClientConnectionManager connectionManager;
    protected ClientPartitionService partitionService;
    protected ClientExecutionService executionService;
    protected ClientListenerServiceImpl clientListenerService;
    protected final ILogger invocationLogger;
    private ResponseThread responseThread;
    private ConcurrentMap<Long, ClientInvocation> callIdMap
            = new ConcurrentHashMap<Long, ClientInvocation>();

    private final CallIdSequence callIdSequence;
    private ClientExceptionFactory clientExceptionFactory;
    private volatile boolean isShutdown;


    public ClientInvocationServiceSupport(HazelcastClientInstanceImpl client) {
        this.client = client;
        int maxAllowedConcurrentInvocations = client.getClientProperties().getInteger(MAX_CONCURRENT_INVOCATIONS);
        callIdSequence = new CallIdSequence.CallIdSequenceFailFast(maxAllowedConcurrentInvocations);
        invocationLogger = client.getLoggingService().getLogger(ClientInvocationService.class);
    }

    @Override
    public void start() {
        connectionManager = client.getConnectionManager();
        executionService = client.getClientExecutionService();
        clientListenerService = (ClientListenerServiceImpl) client.getListenerService();
        connectionManager.addConnectionListener(this);
        connectionManager.addConnectionHeartbeatListener(this);
        partitionService = client.getClientPartitionService();
        clientExceptionFactory = initClientExceptionFactory();
        responseThread = new ResponseThread(client.getThreadGroup(), client.getName() + ".response-",
                client.getClientConfig().getClassLoader());
        responseThread.start();
    }


    private ClientExceptionFactory initClientExceptionFactory() {
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        boolean jcacheAvailable = ClassLoaderUtil.isClassAvailable(classLoader, "javax.cache.Caching");
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

    public void cleanResources(ConstructorFunction<Object, Throwable> responseCtor, ClientConnection connection) {
        final Iterator<Map.Entry<Long, ClientInvocation>> iter = callIdMap.entrySet().iterator();
        while (iter.hasNext()) {
            final Map.Entry<Long, ClientInvocation> entry = iter.next();
            final ClientInvocation invocation = entry.getValue();
            if (connection.equals(invocation.getSendConnection())) {
                iter.remove();
                invocation.notifyException(responseCtor.createNew(null));
            }
        }
    }

    @Override
    public void connectionAdded(Connection connection) {

    }

    @Override
    public void connectionRemoved(Connection connection) {
        cleanConnectionResources((ClientConnection) connection);
    }

    @Override
    public void heartBeatStarted(Connection connection) {

    }

    @Override
    public void heartBeatStopped(Connection connection) {
        cleanConnectionResources((ClientConnection) connection);
    }

    @Override
    public void cleanConnectionResources(ClientConnection connection) {
        if (connectionManager.isAlive()) {
            try {
                ((ClientExecutionServiceImpl) executionService).executeInternal(new CleanResourcesTask(connection));
            } catch (RejectedExecutionException e) {
                invocationLogger.warning("Execution rejected ", e);
            }
        } else {
            cleanResources(new ConstructorFunction<Object, Throwable>() {
                @Override
                public Throwable createNew(Object arg) {
                    return new HazelcastClientNotActiveException("Client is shutting down!");
                }
            }, connection);
        }
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    public void shutdown() {
        isShutdown = true;
        responseThread.interrupt();
    }

    private class CleanResourcesTask implements Runnable {

        private final ClientConnection connection;

        CleanResourcesTask(ClientConnection connection) {
            this.connection = connection;
        }

        @Override
        public void run() {
            waitForPacketsProcessed();
            cleanResources(new ConstructorFunction<Object, Throwable>() {
                @Override
                public Throwable createNew(Object arg) {
                    return new TargetDisconnectedException(connection.getRemoteEndpoint());
                }
            }, connection);
        }

        private void waitForPacketsProcessed() {
            final long begin = System.currentTimeMillis();
            int count = connection.getPendingPacketCount();
            while (count != 0) {
                try {
                    Thread.sleep(WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED);
                } catch (InterruptedException e) {
                    invocationLogger.warning(e);
                    break;
                }
                long elapsed = System.currentTimeMillis() - begin;
                if (elapsed > WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED_THRESHOLD) {
                    invocationLogger.warning("There are packets which are not processed " + count);
                    break;
                }
                count = connection.getPendingPacketCount();
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

        private void handleClientMessage(ClientMessage clientMessage) throws ClassNotFoundException,
                NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
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
