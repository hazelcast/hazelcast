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

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientRemoveAllListenersCodec;
import com.hazelcast.client.impl.protocol.parameters.ErrorCodec;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;


abstract class ClientInvocationServiceSupport implements ClientInvocationService,
        ConnectionHeartbeatListener, ConnectionListener {

    private static final int WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED = 10;
    private static final int WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED_THRESHOLD = 5000;
    protected final HazelcastClientInstanceImpl client;
    protected final ClientConnectionManager connectionManager;
    protected final ClientPartitionService partitionService;
    protected final ClientExecutionService executionService;
    private final ILogger logger = Logger.getLogger(ClientInvocationService.class);
    private final ResponseThread responseThread;
    private final ConcurrentMap<Integer, ClientInvocation> callIdMap
            = new ConcurrentHashMap<Integer, ClientInvocation>();
    private final ConcurrentMap<Integer, ClientListenerInvocation> eventHandlerMap
            = new ConcurrentHashMap<Integer, ClientListenerInvocation>();
    private final AtomicInteger callIdIncrementer = new AtomicInteger();
    private final ClientExceptionFactory clientExceptionFactory;
    private volatile boolean isShutdown;


    public ClientInvocationServiceSupport(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.connectionManager = client.getConnectionManager();
        this.executionService = client.getClientExecutionService();
        connectionManager.addConnectionListener(this);
        connectionManager.addConnectionHeartbeatListener(this);
        this.partitionService = client.getClientPartitionService();
        this.clientExceptionFactory = initClientExceptionFactory();
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
            final int callId = clientMessage.getCorrelationId();
            ClientInvocation clientInvocation = deRegisterCallId(callId);
            deRegisterEventHandler(callId);
            if (clientInvocation != null) {
                throw new IOException("Packet not send to " + connection.getRemoteEndpoint());
            } else {
                if (logger.isFinestEnabled()) {
                    logger.finest("Invocation not found to deregister for call id " + callId);
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

            if (logger.isFinestEnabled()) {
                logger.warning("Connection is not heart-beating, won't write client message -> "
                        + invocation.getClientMessage());
            }
            return false;
        }
        return true;
    }

    private void registerInvocation(ClientInvocation clientInvocation) {
        short protocolVersion = client.getProtocolVersion();
        final int correlationId = newCorrelationId();
        clientInvocation.getClientMessage().setCorrelationId(correlationId).setVersion(protocolVersion);
        callIdMap.put(correlationId, clientInvocation);
        if (clientInvocation instanceof ClientListenerInvocation) {
            eventHandlerMap.put(correlationId, (ClientListenerInvocation) clientInvocation);
        }
    }

    private ClientInvocation deRegisterCallId(int callId) {
        return callIdMap.remove(callId);
    }

    private ClientInvocation deRegisterEventHandler(int callId) {
        return eventHandlerMap.remove(callId);
    }

    @Override
    public EventHandler getEventHandler(int callId) {
        final ClientListenerInvocation clientInvocation = eventHandlerMap.get(callId);
        if (clientInvocation == null) {
            return null;
        }
        return clientInvocation.getHandler();
    }

    @Override
    public boolean removeEventHandler(Integer callId) {
        if (callId != null) {
            return eventHandlerMap.remove(callId) != null;

        }
        return false;
    }


    public void cleanResources(ConstructorFunction<Object, Throwable> responseCtor, ClientConnection connection) {
        final Iterator<Map.Entry<Integer, ClientInvocation>> iter = callIdMap.entrySet().iterator();
        while (iter.hasNext()) {
            final Map.Entry<Integer, ClientInvocation> entry = iter.next();
            final ClientInvocation invocation = entry.getValue();
            if (connection.equals(invocation.getSendConnection())) {
                iter.remove();
                invocation.notifyException(responseCtor.createNew(null));
                eventHandlerMap.remove(entry.getKey());
            }
        }
        final Iterator<ClientListenerInvocation> iterator = eventHandlerMap.values().iterator();
        while (iterator.hasNext()) {
            final ClientInvocation invocation = iterator.next();
            if (connection.equals(invocation.getSendConnection())) {
                iterator.remove();
                invocation.notifyException(responseCtor.createNew(null));
            }
        }

    }

    @Override
    public void heartBeatStarted(Connection connection) {

    }

    @Override
    public void heartBeatStopped(Connection connection) {
        ClientMessage request = ClientRemoveAllListenersCodec.encodeRequest();
        ClientInvocation removeListenerInvocation = new ClientInvocation(client, request, connection);
        removeListenerInvocation.setBypassHeartbeatCheck(true);
        removeListenerInvocation.invoke();

        final Address remoteEndpoint = connection.getEndPoint();
        final Iterator<ClientListenerInvocation> iterator = eventHandlerMap.values().iterator();
        final TargetDisconnectedException response = new TargetDisconnectedException(remoteEndpoint);

        while (iterator.hasNext()) {
            final ClientInvocation clientInvocation = iterator.next();
            if (clientInvocation.getSendConnection().equals(connection)) {
                iterator.remove();
                clientInvocation.notifyException(response);
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
    public void cleanConnectionResources(ClientConnection connection) {
        if (connectionManager.isAlive()) {
            try {
                executionService.execute(new CleanResourcesTask(connection));
            } catch (RejectedExecutionException e) {
                logger.warning("Execution rejected ", e);
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
            int count = connection.getPacketCount();
            while (count != 0) {
                try {
                    Thread.sleep(WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED);
                } catch (InterruptedException e) {
                    logger.warning(e);
                    break;
                }
                long elapsed = System.currentTimeMillis() - begin;
                if (elapsed > WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED_THRESHOLD) {
                    logger.warning("There are packets which are not processed " + count);
                    break;
                }
                count = connection.getPacketCount();
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
                logger.severe(t);
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
                logger.severe("Failed to process task: " + packet + " on responseThread :" + getName(), e);
            } finally {
                conn.decrementPacketCount();
            }
        }

        private void handleClientMessage(ClientMessage clientMessage) throws ClassNotFoundException,
                NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
            int correlationId = clientMessage.getCorrelationId();

            final ClientInvocation future = deRegisterCallId(correlationId);
            if (future == null) {
                logger.warning("No call for callId: " + correlationId + ", response: " + clientMessage);
                return;
            }

            if (ErrorCodec.TYPE == clientMessage.getMessageType()) {
                ErrorCodec exParameters = ErrorCodec.decode(clientMessage);
                Throwable exception =
                        clientExceptionFactory.createException(exParameters.errorCode, exParameters.className,
                                exParameters.message, exParameters.stackTrace,
                                exParameters.causeErrorCode, exParameters.causeClassName);
                future.notifyException(exception);
            } else {
                future.notify(clientMessage);
            }
        }

    }

    private int newCorrelationId() {
        return callIdIncrementer.incrementAndGet();
    }

}
