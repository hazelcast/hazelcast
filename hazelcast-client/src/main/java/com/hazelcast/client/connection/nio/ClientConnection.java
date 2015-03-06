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

package com.hazelcast.client.connection.nio;

import com.hazelcast.client.ClientTypes;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.client.RemoveAllListeners;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientCallFuture;
import com.hazelcast.client.spi.impl.ClientInvocationServiceImpl;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.SocketWritable;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.tcp.IOSelector;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.StringUtil.stringToBytes;

public class ClientConnection implements Connection, Closeable {

    private static final int WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED = 10;

    private volatile boolean live = true;

    private final ILogger logger = Logger.getLogger(ClientConnection.class);

    private final ClientWriteHandler writeHandler;

    private final ClientReadHandler readHandler;

    private final ClientConnectionManager connectionManager;

    private final int connectionId;

    private final SocketChannelWrapper socketChannelWrapper;

    private volatile Address remoteEndpoint;

    private final ConcurrentMap<Integer, ClientCallFuture> callIdMap
            = new ConcurrentHashMap<Integer, ClientCallFuture>();
    private final ConcurrentMap<Integer, ClientCallFuture> eventHandlerMap
            = new ConcurrentHashMap<Integer, ClientCallFuture>();
    private final ByteBuffer readBuffer;
    private final SerializationService serializationService;
    private final ClientExecutionService executionService;
    private final ClientInvocationServiceImpl invocationService;
    private boolean readFromSocket = true;
    private final AtomicInteger packetCount = new AtomicInteger(0);
    private volatile boolean heartBeating = true;

    public ClientConnection(ClientConnectionManager connectionManager, IOSelector in, IOSelector out,
                            int connectionId, SocketChannelWrapper socketChannelWrapper,
                            ClientExecutionService executionService,
                            ClientInvocationServiceImpl invocationService,
                            SerializationService serializationService) throws IOException {
        final Socket socket = socketChannelWrapper.socket();
        this.connectionManager = connectionManager;
        this.serializationService = serializationService;
        this.executionService = executionService;
        this.invocationService = invocationService;
        this.socketChannelWrapper = socketChannelWrapper;
        this.connectionId = connectionId;
        this.readHandler = new ClientReadHandler(this, in, socket.getReceiveBufferSize());
        this.writeHandler = new ClientWriteHandler(this, out, socket.getSendBufferSize());
        this.readBuffer = ByteBuffer.allocate(socket.getReceiveBufferSize());
    }

    public void incrementPacketCount() {
        packetCount.incrementAndGet();
    }

    public void decrementPacketCount() {
        packetCount.decrementAndGet();
    }

    public void registerCallId(ClientCallFuture future) {
        final int callId = connectionManager.newCallId();
        future.getRequest().setCallId(callId);
        callIdMap.put(callId, future);
        if (future.getHandler() != null) {
            eventHandlerMap.put(callId, future);
        }
    }

    public ClientCallFuture deRegisterCallId(int callId) {
        return callIdMap.remove(callId);
    }

    public ClientCallFuture deRegisterEventHandler(int callId) {
        return eventHandlerMap.remove(callId);
    }

    public EventHandler getEventHandler(int callId) {
        final ClientCallFuture future = eventHandlerMap.get(callId);
        if (future == null) {
            return null;
        }
        return future.getHandler();
    }

    public SerializationService getSerializationService() {
        return serializationService;
    }

    @Override
    public boolean write(SocketWritable packet) {
        if (!live) {
            if (logger.isFinestEnabled()) {
                logger.finest("Connection is closed, won't write packet -> " + packet);
            }
            return false;
        }
        writeHandler.enqueueSocketWritable(packet);
        return true;
    }

    public void init() throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(6);
        buffer.put(stringToBytes(Protocols.CLIENT_BINARY));
        buffer.put(stringToBytes(ClientTypes.JAVA));
        buffer.flip();
        socketChannelWrapper.write(buffer);
    }

    public void write(Data data) throws IOException {
        final Packet packet = new Packet(data, serializationService.getPortableContext());
        final int totalSize = packet.size();
        final int bufferSize = SocketOptions.DEFAULT_BUFFER_SIZE_BYTE;
        final ByteBuffer buffer = ByteBuffer.allocate(totalSize > bufferSize ? bufferSize : totalSize);
        boolean complete = false;
        while (!complete) {
            complete = packet.writeTo(buffer);
            buffer.flip();
            try {
                socketChannelWrapper.write(buffer);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
            buffer.clear();
        }
    }

    public Data read() throws IOException {
        Packet packet = new Packet(serializationService.getPortableContext());
        while (true) {
            if (readFromSocket) {
                int readBytes = socketChannelWrapper.read(readBuffer);
                if (readBytes == -1) {
                    throw new EOFException("Remote socket closed!");
                }
                readBuffer.flip();
            }
            boolean complete = packet.readFrom(readBuffer);
            if (complete) {
                if (readBuffer.hasRemaining()) {
                    readFromSocket = false;
                } else {
                    readBuffer.compact();
                    readFromSocket = true;
                }
                return packet.getData();
            }
            readFromSocket = true;
            readBuffer.clear();
        }
    }

    @Override
    public Address getEndPoint() {
        return remoteEndpoint;
    }

    @Override
    public boolean isAlive() {
        return live;
    }

    @Override
    public long lastReadTime() {
        return readHandler.getLastHandle();
    }

    @Override
    public long lastWriteTime() {
        return writeHandler.getLastHandle();
    }

    @Override
    public void close() {
        close(null);
    }

    @Override
    public ConnectionType getType() {
        return ConnectionType.JAVA_CLIENT;
    }

    @Override
    public boolean isClient() {
        return true;
    }

    @Override
    public InetAddress getInetAddress() {
        return socketChannelWrapper.socket().getInetAddress();
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress) socketChannelWrapper.socket().getRemoteSocketAddress();
    }

    @Override
    public int getPort() {
        return socketChannelWrapper.socket().getPort();
    }

    public SocketChannelWrapper getSocketChannelWrapper() {
        return socketChannelWrapper;
    }

    public ClientConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public ClientReadHandler getReadHandler() {
        return readHandler;
    }

    public void setRemoteEndpoint(Address remoteEndpoint) {
        this.remoteEndpoint = remoteEndpoint;
    }

    public Address getRemoteEndpoint() {
        return remoteEndpoint;
    }

    public InetSocketAddress getLocalSocketAddress() {
        return (InetSocketAddress) socketChannelWrapper.socket().getLocalSocketAddress();
    }

    private void innerClose() throws IOException {
        if (!live) {
            return;
        }
        live = false;
        if (socketChannelWrapper.isOpen()) {
            socketChannelWrapper.close();
        }
        readHandler.shutdown();
        writeHandler.shutdown();
        if (socketChannelWrapper.isBlocking()) {
            return;
        }
        if (connectionManager.isAlive()) {
            try {
                executionService.execute(new CleanResourcesTask());
            } catch (RejectedExecutionException e) {
                logger.warning("Execution rejected ", e);
            }
        } else {
            cleanResources(new ConstructorFunction<Object, Throwable>() {
                @Override
                public Throwable createNew(Object arg) {
                    return new HazelcastException("Client is shutting down!");
                }
            });
        }
    }

    private class CleanResourcesTask implements Runnable {
        @Override
        public void run() {
            waitForPacketsProcessed();
            cleanResources(new ConstructorFunction<Object, Throwable>() {
                @Override
                public Throwable createNew(Object arg) {
                    return new TargetDisconnectedException(remoteEndpoint);
                }
            });
        }

        private void waitForPacketsProcessed() {
            final long begin = System.currentTimeMillis();
            int count = packetCount.get();
            while (count != 0) {
                try {
                    Thread.sleep(WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED);
                } catch (InterruptedException e) {
                    logger.warning(e);
                    break;
                }
                long elapsed = System.currentTimeMillis() - begin;
                if (elapsed > WAIT_TIME_FOR_PACKETS_TO_BE_CONSUMED / 2) {
                    logger.warning("There are packets which are not processed " + count);
                    break;
                }
                count = packetCount.get();
            }
        }
    }

    private void cleanResources(ConstructorFunction<Object, Throwable> responseCtor) {
        final Iterator<Map.Entry<Integer, ClientCallFuture>> iter = callIdMap.entrySet().iterator();
        while (iter.hasNext()) {
            final Map.Entry<Integer, ClientCallFuture> entry = iter.next();
            iter.remove();
            entry.getValue().notify(responseCtor.createNew(null));
            eventHandlerMap.remove(entry.getKey());
        }
        final Iterator<ClientCallFuture> iterator = eventHandlerMap.values().iterator();
        while (iterator.hasNext()) {
            final ClientCallFuture future = iterator.next();
            iterator.remove();
            future.notify(responseCtor.createNew(null));
        }
    }

    public void close(Throwable t) {
        if (!live) {
            return;
        }
        try {
            innerClose();
        } catch (Exception e) {
            logger.warning(e);
        }
        String message = "Connection [" + socketChannelWrapper.socket().getRemoteSocketAddress() + "] lost. Reason: ";
        if (t != null) {
            message += t.getClass().getName() + "[" + t.getMessage() + "]";
        } else {
            message += "Socket explicitly closed";
        }

        logger.warning(message);
        if (!socketChannelWrapper.isBlocking()) {
            connectionManager.onConnectionClose(this);
        }
    }

    //failedHeartBeat is incremented in single thread.
    @edu.umd.cs.findbugs.annotations.SuppressWarnings("VO_VOLATILE_INCREMENT")
    void heartBeatingFailed() {
        if (!heartBeating) {
            return;
        }
        final RemoveAllListeners request = new RemoveAllListeners();
        invocationService.send(request, ClientConnection.this);
        heartBeating = false;
        connectionManager.onDetectingUnresponsiveConnection(this);
        final Iterator<ClientCallFuture> iterator = eventHandlerMap.values().iterator();
        final TargetDisconnectedException response = new TargetDisconnectedException(remoteEndpoint);

        while (iterator.hasNext()) {
            final ClientCallFuture future = iterator.next();
            iterator.remove();
            future.notify(response);
        }
    }

    void heartBeatingSucceed() {
        heartBeating = true;
    }

    public boolean isHeartBeating() {
        return live && heartBeating;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClientConnection)) {
            return false;
        }

        ClientConnection that = (ClientConnection) o;

        if (connectionId != that.connectionId) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return connectionId;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientConnection{");
        sb.append("live=").append(live);
        sb.append(", writeHandler=").append(writeHandler);
        sb.append(", readHandler=").append(readHandler);
        sb.append(", connectionId=").append(connectionId);
        sb.append(", socketChannel=").append(socketChannelWrapper);
        sb.append(", remoteEndpoint=").append(remoteEndpoint);
        sb.append('}');
        return sb.toString();
    }

}
