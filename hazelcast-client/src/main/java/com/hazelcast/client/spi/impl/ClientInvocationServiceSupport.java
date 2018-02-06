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
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.impl.client.ClientResponse;
import com.hazelcast.client.impl.client.RemoveAllListeners;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.cluster.client.ClientPingRequest;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.exception.TargetDisconnectedException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;


abstract class ClientInvocationServiceSupport implements ClientInvocationService {

    protected final HazelcastClientInstanceImpl client;
    protected final ClientConnectionManager connectionManager;
    protected final ClientPartitionService partitionService;
    protected final ClientExecutionService executionService;
    private final ILogger logger = Logger.getLogger(ClientInvocationService.class);
    private final ResponseThread responseThread;
    private final ConcurrentMap<Integer, ClientInvocation> invocations
            = new ConcurrentHashMap<Integer, ClientInvocation>();
    private final ConcurrentMap<Integer, ClientInvocation> eventHandlerMap
            = new ConcurrentHashMap<Integer, ClientInvocation>();
    private final AtomicInteger callIdIncrementer = new AtomicInteger();

    private volatile boolean isShutdown;


    public ClientInvocationServiceSupport(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.connectionManager = client.getConnectionManager();
        this.executionService = client.getClientExecutionService();
        this.partitionService = client.getClientPartitionService();
        responseThread = new ResponseThread(client.getThreadGroup(), client.getName() + ".response-",
                client.getClientConfig().getClassLoader());
        responseThread.start();
        executionService.scheduleAtFixedRate(new CleanResourcesTask(), 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public <T> ICompletableFuture<T> invokeOnTarget(ClientRequest request, Address target) throws Exception {
        return new ClientInvocation(client, request, target).invoke();
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
        final SerializationService ss = client.getSerializationService();
        final Data data = ss.toData(invocation.getRequest());
        Packet packet = new Packet(data, invocation.getPartitionId());
        if (!isAllowedToSendRequest(connection, invocation.getRequest()) || !connection.write(packet)) {
            final int callId = invocation.getRequest().getCallId();
            deRegisterCallId(callId);
            deRegisterEventHandler(callId);
            throw new IOException("Packet not send to " + connection.getRemoteEndpoint());
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
        final int callId = newCallId();
        clientInvocation.getRequest().setCallId(callId);
        invocations.put(callId, clientInvocation);
        if (clientInvocation.getHandler() != null) {
            eventHandlerMap.put(callId, clientInvocation);
        }
    }

    private ClientInvocation deRegisterCallId(int callId) {
        return invocations.remove(callId);
    }

    private ClientInvocation deRegisterEventHandler(int callId) {
        return eventHandlerMap.remove(callId);
    }

    @Override
    public EventHandler getEventHandler(int callId) {
        final ClientInvocation clientInvocation = eventHandlerMap.get(callId);
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

    public boolean isShutdown() {
        return isShutdown;
    }

    public void shutdown() {
        isShutdown = true;
        responseThread.interrupt();
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
            Iterator<Map.Entry<Integer, ClientInvocation>> invocationIterator = invocations.entrySet().iterator();
            while (invocationIterator.hasNext()) {
                Map.Entry<Integer, ClientInvocation> entry = invocationIterator.next();
                ClientInvocation invocation = entry.getValue();
                ClientConnection connection = invocation.getSendConnection();
                if (connection == null) {
                    continue;
                }

                if (connection.isHeartBeating()) {
                    continue;
                }

                invocationIterator.remove();
                eventHandlerMap.remove(entry.getKey());
                notifyException(invocation, connection);
            }
            Iterator<Map.Entry<Integer, ClientInvocation>> eventHandlerIter = eventHandlerMap.entrySet().iterator();
            while (eventHandlerIter.hasNext()) {
                Map.Entry<Integer, ClientInvocation> entry = eventHandlerIter.next();
                ClientInvocation invocation = entry.getValue();
                ClientConnection connection = invocation.getSendConnection();
                if (connection == null) {
                    continue;
                }

                if (connection.isHeartBeating()) {
                    continue;
                }

                eventHandlerIter.remove();
                notifyException(invocation, connection);
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
                ex = new TargetDisconnectedException("Connection closed : " + connection);
            } else {
                ex = new TargetDisconnectedException("Heartbeat timed out to " + connection);
            }

            invocation.notifyException(ex);
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
            final ClientConnection conn = (ClientConnection) packet.getConn();
            try {
                final ClientResponse clientResponse = client.getSerializationService().toObject(packet.getData());
                final int callId = clientResponse.getCallId();
                final Data response = clientResponse.getResponse();
                handlePacket(response, clientResponse.isError(), callId);
            } catch (Exception e) {
                logger.severe("Failed to process task: " + packet + " on responseThread :" + getName());
            } finally {
                conn.decrementPendingPacketCount();
            }
        }

        private void handlePacket(Object response, boolean isError, int callId) {
            final ClientInvocation future = deRegisterCallId(callId);
            if (future == null) {
                logger.warning("No call for callId: " + callId + ", response: " + response);
                return;
            }
            if (isError) {
                response = client.getSerializationService().toObject(response);
            }
            future.notify(response);
        }

    }

    private int newCallId() {
        return callIdIncrementer.incrementAndGet();
    }

}
