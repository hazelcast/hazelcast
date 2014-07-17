/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.ClientRequest;
import com.hazelcast.client.ClientResponse;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.exception.TargetNotMemberException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;

public final class ClientInvocationServiceImpl implements ClientInvocationService {

    private final ILogger logger = Logger.getLogger(ClientInvocationService.class);
    private final HazelcastClient client;
    private final ClientConnectionManager connectionManager;

    private final ResponseThread responseThread;
    private volatile boolean isShutdown;

    public ClientInvocationServiceImpl(HazelcastClient client) {
        this.client = client;
        this.connectionManager = client.getConnectionManager();
        responseThread = new ResponseThread(client.getThreadGroup(), client.getName() + ".response-",
                client.getClientConfig().getClassLoader());
        responseThread.start();
    }

    public <T> ICompletableFuture<T> invokeOnRandomTarget(ClientRequest request) throws Exception {
        return send(request);
    }

    public <T> ICompletableFuture<T> invokeOnTarget(ClientRequest request, Address target) throws Exception {
        return send(request, target);
    }

    public <T> ICompletableFuture<T> invokeOnKeyOwner(ClientRequest request, Object key) throws Exception {
        ClientPartitionServiceImpl partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
        final Address owner = partitionService.getPartitionOwner(partitionService.getPartitionId(key));
        if (owner != null) {
            return invokeOnTarget(request, owner);
        }
        return invokeOnRandomTarget(request);
    }

    public <T> ICompletableFuture<T> invokeOnRandomTarget(ClientRequest request, EventHandler handler) throws Exception {
        return sendAndHandle(request, handler);
    }

    public <T> ICompletableFuture<T> invokeOnTarget(ClientRequest request, Address target, EventHandler handler)
            throws Exception {
        return sendAndHandle(request, target, handler);
    }

    public <T> ICompletableFuture<T> invokeOnKeyOwner(ClientRequest request, Object key, EventHandler handler)
            throws Exception {
        ClientPartitionServiceImpl partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
        final Address owner = partitionService.getPartitionOwner(partitionService.getPartitionId(key));
        if (owner != null) {
            return invokeOnTarget(request, owner, handler);
        }
        return invokeOnRandomTarget(request, handler);
    }

    // NIO public

    public ICompletableFuture send(ClientRequest request, ClientConnection connection) {
        request.setSingleConnection();
        return doSend(request, connection, null);
    }

    public Future reSend(ClientCallFuture future) throws Exception {
        final ClientConnection connection = connectionManager.tryToConnect(null);
        sendInternal(future, connection);
        return future;
    }

    public boolean isRedoOperation() {
        return client.getClientConfig().isRedoOperation();
    }


    //NIO private

    private ICompletableFuture send(ClientRequest request) throws Exception {
        final ClientConnection connection = connectionManager.tryToConnect(null);
        return doSend(request, connection, null);
    }

    private ICompletableFuture send(ClientRequest request, Address target) throws Exception {
        final ClientConnection connection = connectionManager.tryToConnect(target);
        return doSend(request, connection, null);
    }

    private ICompletableFuture sendAndHandle(ClientRequest request, EventHandler handler) throws Exception {
        final ClientConnection connection = connectionManager.tryToConnect(null);
        return doSend(request, connection, handler);
    }

    private ICompletableFuture sendAndHandle(ClientRequest request, Address target, EventHandler handler) throws Exception {
        final ClientConnection connection = connectionManager.tryToConnect(target);
        return doSend(request, connection, handler);
    }

    private ICompletableFuture doSend(ClientRequest request, ClientConnection connection, EventHandler handler) {
        final ClientCallFuture future = new ClientCallFuture(client, request, handler);
        sendInternal(future, connection);
        return future;
    }

    private void sendInternal(ClientCallFuture future, ClientConnection connection) {
        connection.registerCallId(future);
        future.setConnection(connection);
        final SerializationService ss = client.getSerializationService();
        final Data data = ss.toData(future.getRequest());
        if (!connection.write(new Packet(data, ss.getPortableContext()))) {
            final int callId = future.getRequest().getCallId();
            connection.deRegisterCallId(callId);
            connection.deRegisterEventHandler(callId);
            future.notify(new TargetNotMemberException("Address : " + connection.getRemoteEndpoint()));
        }
    }

    public void shutdown() {
        isShutdown = true;
        responseThread.interrupt();
    }

    public void handlePacket(Packet packet) {
        responseThread.workQueue.add(packet);
    }

    private class ResponseThread extends Thread {
        private final BlockingQueue<Packet> workQueue = new LinkedBlockingQueue<Packet>();

        public ResponseThread(ThreadGroup threadGroup, String name, ClassLoader classLoader) {
            super(threadGroup, name);
            setContextClassLoader(classLoader);
        }

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
            for (;;) {
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
            try {
                final ClientConnection conn = (ClientConnection) packet.getConn();
                final ClientResponse clientResponse = client.getSerializationService().toObject(packet.getData());
                final int callId = clientResponse.getCallId();
                final Data response = clientResponse.getResponse();
                handlePacket(response, clientResponse.isError(), callId, conn);
                conn.decrementPacketCount();
            } catch (Exception e) {
                logger.severe("Failed to process task: " + packet + " on responseThread :" + getName());
            }
        }

        private void handlePacket(Object response, boolean isError, int callId, ClientConnection conn) {
            final ClientCallFuture future = conn.deRegisterCallId(callId);
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

}
