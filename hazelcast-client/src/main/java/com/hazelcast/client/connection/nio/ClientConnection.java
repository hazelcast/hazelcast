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

package com.hazelcast.client.connection.nio;

import com.hazelcast.client.ClientTypes;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.SocketWritable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.tcp.IOSelector;
import com.hazelcast.nio.tcp.SocketChannelWrapper;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.StringUtil.stringToBytes;

public class ClientConnection implements Connection, Closeable {

    private final ILogger logger = Logger.getLogger(ClientConnection.class);
    private final AtomicBoolean live = new AtomicBoolean(true);
    private final AtomicInteger packetCount = new AtomicInteger(0);

    private final ClientWriteHandler writeHandler;
    private final ClientReadHandler readHandler;
    private final int connectionId;
    private final SocketChannelWrapper socketChannelWrapper;
    private final ClientConnectionManager connectionManager;
    private final SerializationService serializationService;

    private volatile Address remoteEndpoint;
    private volatile boolean heartBeating = true;

    public ClientConnection(HazelcastClientInstanceImpl client, IOSelector in, IOSelector out,
                            int connectionId, SocketChannelWrapper socketChannelWrapper) throws IOException {
        final Socket socket = socketChannelWrapper.socket();
        this.connectionManager = client.getConnectionManager();
        this.serializationService = client.getSerializationService();
        this.socketChannelWrapper = socketChannelWrapper;
        this.connectionId = connectionId;
        this.readHandler = new ClientReadHandler(this, in, socket.getReceiveBufferSize());
        this.writeHandler = new ClientWriteHandler(this, out, socket.getSendBufferSize());
    }

    public void incrementPacketCount() {
        packetCount.incrementAndGet();
    }

    public void decrementPacketCount() {
        packetCount.decrementAndGet();
    }

    public int getPacketCount() {
        return packetCount.get();
    }

    public SerializationService getSerializationService() {
        return serializationService;
    }

    @Override
    public boolean write(SocketWritable packet) {
        if (!live.get()) {
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

    @Override
    public Address getEndPoint() {
        return remoteEndpoint;
    }

    @Override
    public boolean isAlive() {
        return live.get();
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
    }

    //failedHeartBeat is incremented in single thread.
    @edu.umd.cs.findbugs.annotations.SuppressWarnings("VO_VOLATILE_INCREMENT")
    void heartBeatingFailed() {
        heartBeating = false;
    }

    void heartBeatingSucceed() {
        heartBeating = true;
    }

    public boolean isHeartBeating() {
        return heartBeating;
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
