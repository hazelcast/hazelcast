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

package com.hazelcast.client.connection.nio;

import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThread;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.StringUtil.stringToBytes;

public class ClientConnection implements Connection {

    protected final int connectionId;
    private final AtomicBoolean live = new AtomicBoolean(true);
    private final ILogger logger;

    private final AtomicInteger pendingPacketCount = new AtomicInteger(0);
    private final ClientWriteHandler writeHandler;
    private final ClientReadHandler readHandler;
    private final SocketChannelWrapper socketChannelWrapper;
    private final ClientConnectionManager connectionManager;
    private final SerializationService serializationService;
    private final LifecycleService lifecycleService;

    private volatile Address remoteEndpoint;
    private volatile boolean heartBeating = true;
    private boolean isAuthenticatedAsOwner;

    public ClientConnection(HazelcastClientInstanceImpl client, NonBlockingIOThread in, NonBlockingIOThread out,
                            int connectionId, SocketChannelWrapper socketChannelWrapper) throws IOException {
        final Socket socket = socketChannelWrapper.socket();
        this.connectionManager = client.getConnectionManager();
        this.serializationService = client.getSerializationService();
        this.lifecycleService = client.getLifecycleService();
        this.socketChannelWrapper = socketChannelWrapper;
        this.connectionId = connectionId;
        LoggingService clientLoggingService = client.getLoggingService();
        this.logger = clientLoggingService.getLogger(ClientConnection.class);
        boolean directBuffer = client.getClientProperties().getBoolean(GroupProperty.SOCKET_CLIENT_BUFFER_DIRECT);
        this.readHandler = new ClientReadHandler(this, in, socket.getReceiveBufferSize(), directBuffer, clientLoggingService);
        this.writeHandler = new ClientWriteHandler(this, out, socket.getSendBufferSize(), directBuffer, clientLoggingService);
    }

    public ClientConnection(HazelcastClientInstanceImpl client,
                            int connectionId) throws IOException {
        this.connectionManager = client.getConnectionManager();
        this.serializationService = client.getSerializationService();
        this.lifecycleService = client.getLifecycleService();
        this.connectionId = connectionId;
        writeHandler = null;
        readHandler = null;
        socketChannelWrapper = null;
        logger = client.getLoggingService().getLogger(ClientConnection.class);
    }

    public void incrementPendingPacketCount() {
        pendingPacketCount.incrementAndGet();
    }

    public void decrementPendingPacketCount() {
        pendingPacketCount.decrementAndGet();
    }

    public int getPendingPacketCount() {
        return pendingPacketCount.get();
    }

    public SerializationService getSerializationService() {
        return serializationService;
    }

    @Override
    public boolean write(OutboundFrame frame) {
        if (!live.get()) {
            if (logger.isFinestEnabled()) {
                logger.finest("Connection is closed, dropping frame -> " + frame);
            }
            return false;
        }
        writeHandler.enqueue(frame);
        return true;
    }

    public void init() throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put(stringToBytes(Protocols.CLIENT_BINARY_NEW));
        buffer.flip();
        socketChannelWrapper.write(buffer);
    }

    @Override
    public Address getEndPoint() {
        return remoteEndpoint;
    }

    @Override
    public boolean isAlive() {
        return live.get();
    }

    @Override
    public long lastReadTimeMillis() {
        return readHandler.getLastHandle();
    }

    @Override
    public long lastWriteTimeMillis() {
        return writeHandler.getLastHandle();
    }

    @Override
    public void close() {
        close(null);
    }

    @Override
    public void setType(ConnectionType type) {
        //NO OP
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

    protected void innerClose() throws IOException {
        if (socketChannelWrapper.isOpen()) {
            socketChannelWrapper.close();
        }
        readHandler.shutdown();
        writeHandler.shutdown();
    }

    public void close(Throwable t) {
        if (!live.compareAndSet(true, false)) {
            return;
        }
        String message = "Connection [" + getRemoteSocketAddress() + "] lost. Reason: ";
        if (t != null) {
            message += t.getClass().getName() + '[' + t.getMessage() + ']';
        } else {
            message += "Socket explicitly closed";
        }

        try {
            innerClose();
        } catch (Exception e) {
            logger.warning(e);
        }

        if (lifecycleService.isRunning()) {
            logger.warning(message);
        } else {
            logger.finest(message);
        }
    }

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "incremented in single thread")
    void heartBeatingFailed() {
        heartBeating = false;
    }

    void heartBeatingSucceed() {
        heartBeating = true;
    }

    public boolean isHeartBeating() {
        return live.get() && heartBeating;
    }

    public boolean isAuthenticatedAsOwner() {
        return isAuthenticatedAsOwner;
    }

    public void setIsAuthenticatedAsOwner() {
        this.isAuthenticatedAsOwner = true;
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
        return "ClientConnection{"
                + "live=" + live
                + ", writeHandler=" + writeHandler
                + ", readHandler=" + readHandler
                + ", connectionId=" + connectionId
                + ", socketChannel=" + socketChannelWrapper
                + ", remoteEndpoint=" + remoteEndpoint
                + '}';
    }
}
