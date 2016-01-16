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
import com.hazelcast.client.config.ClientProperties;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.impl.client.ClientResponse;
import com.hazelcast.client.impl.client.RemoveAllListeners;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.cluster.client.ClientPingRequest;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.util.ConstructorFunction;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;

import static com.hazelcast.client.config.ClientProperty.HEARTBEAT_INTERVAL;
import static com.hazelcast.client.config.ClientProperty.INVOCATION_TIMEOUT_SECONDS;
import static com.hazelcast.client.config.ClientProperty.MAX_CONCURRENT_INVOCATIONS;
import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;


abstract class ClientInvocationServiceSupport implements ClientInvocationService, ConnectionListener {


    private static final int WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED = 10;
    private static final int WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED_THRESHOLD = 5000;
    protected final HazelcastClientInstanceImpl client;
    protected ClientConnectionManager connectionManager;
    protected ClientPartitionService partitionService;
    protected ClientExecutionService executionService;
    protected ClientListenerServiceImpl clientListenerService;
    private ILogger logger = Logger.getLogger(ClientInvocationService.class);
    private ResponseThread responseThread;
    private final ConcurrentMap<Long, ClientInvocation> callIdMap
            = new ConcurrentHashMap<Long, ClientInvocation>();
    private final long heartBeatIntervalMillis;
    private final long clientInvocationTimeoutMillis;
    private final CallIdSequence callIdSequence;
    private volatile boolean isShutdown;


    public ClientInvocationServiceSupport(HazelcastClientInstanceImpl client) {
        this.client = client;
        int maxAllowedConcurrentInvocations = client.getClientProperties().getInteger(MAX_CONCURRENT_INVOCATIONS);
        callIdSequence = new CallIdSequence.CallIdSequenceFailFast(maxAllowedConcurrentInvocations);

        ClientProperties clientProperties = client.getClientProperties();
        clientInvocationTimeoutMillis = clientProperties.getMillis(INVOCATION_TIMEOUT_SECONDS);
        heartBeatIntervalMillis = clientProperties.getMillis(HEARTBEAT_INTERVAL);
    }

    @Override
    public void start() {
        connectionManager = client.getConnectionManager();
        executionService = client.getClientExecutionService();
        clientListenerService = (ClientListenerServiceImpl) client.getListenerService();
        connectionManager.addConnectionListener(this);
        partitionService = client.getClientPartitionService();
        responseThread = new ResponseThread(client.getThreadGroup(), client.getName() + ".response-",
                client.getClientConfig().getClassLoader());
        responseThread.start();
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnTarget(ClientRequest request, Address target) throws Exception {
        return new ClientInvocation(client, request, target).invoke();
    }

    @Override
    public boolean isRedoOperation() {
        return client.getClientConfig().getNetworkConfig().isRedoOperation();
    }

    @Override
    public long getHeartBeatIntervalMillis() {
        return heartBeatIntervalMillis;
    }

    @Override
    public long getClientInvocationTimeoutMillis() {
        return clientInvocationTimeoutMillis;
    }

    protected void send(ClientInvocation invocation, ClientConnection connection) throws IOException {
        if (isShutdown) {
            throw new HazelcastClientNotActiveException("Client is shut down");
        }
        registerInvocation(invocation);
        final SerializationService ss = client.getSerializationService();
        final byte[] bytes = ss.toBytes(invocation.getRequest());
        Packet packet = new Packet(bytes, invocation.getPartitionId());
        if (!isAllowedToSendRequest(connection, invocation.getRequest()) || !connection.write(packet)) {
            long callId = invocation.getRequest().getCallId();
            ClientInvocation clientInvocation = deRegisterCallId(callId);
            if (clientInvocation != null) {
                callIdSequence.complete();
                throw new IOException("Packet not send to " + connection.getRemoteEndpoint());
            } else {
                if (logger.isFinestEnabled()) {
                    logger.finest("Invocation not found to deregister for call id " + callId);
                }
            }
        }

        invocation.setSendConnection(connection);
    }

    private boolean isAllowedToSendRequest(ClientConnection connection, ClientRequest request) {
        if (!connection.isHeartBeating()) {
            if (request instanceof ClientPingRequest || request instanceof RemoveAllListeners) {
                //ping request and removeAllListeners should be send even though heart is not beating
                return true;
            }

            if (logger.isFinestEnabled()) {
                logger.warning("Connection is not heart-beating, won't write request -> " + request);
            }
            return false;
        }
        return true;
    }

    private void registerInvocation(ClientInvocation clientInvocation) {
        long callId;
        if (clientInvocation.isUrgent()) {
            callId = callIdSequence.renew();
        } else {
            callId = callIdSequence.next();
        }
        clientInvocation.getRequest().setCallId(callId);
        callIdMap.put(callId, clientInvocation);
        EventHandler handler = clientInvocation.getEventHandler();
        if (handler != null) {
            clientListenerService.addEventHandler(callId, handler);
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
                invocation.notify(responseCtor.createNew(null));
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
                ((ClientExecutionServiceImpl) executionService).executeInternal(new CleanResourcesTask(connection));
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
            int count = connection.getPendingPacketCount();
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
                count = connection.getPendingPacketCount();
            }
        }
    }


    @Override
    public void handlePacket(Packet packet) {
        responseThread.workQueue.add(packet);
    }

    private class ResponseThread extends Thread {
        private final BlockingQueue<Packet> workQueue = new LinkedBlockingQueue<Packet>();

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
                Packet task;
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

        private void process(Packet packet) {
            ClientConnection conn = (ClientConnection) packet.getConn();
            try {
                ClientResponse clientResponse = client.getSerializationService().toObject(packet);
                long callId = clientResponse.getCallId();
                Data response = clientResponse.getResponse();
                //TODO can response be made to be NULL ?
                if (response == null) {
                    response = new HeapData();
                }
                handlePacket(response, clientResponse.isError(), callId);
            } catch (Exception e) {
                logger.severe("Failed to process task: " + packet + " on responseThread :" + getName(), e);
            } finally {
                conn.decrementPendingPacketCount();
            }
        }

        private void handlePacket(Object response, boolean isError, long callId) {
            ClientInvocation future = deRegisterCallId(callId);
            if (future == null) {
                logger.warning("No call for callId: " + callId + ", response: " + response);
                return;
            }
            callIdSequence.complete();
            if (isError) {
                response = client.getSerializationService().toObject(response);
            }
            future.notify(response);
        }

    }

}
