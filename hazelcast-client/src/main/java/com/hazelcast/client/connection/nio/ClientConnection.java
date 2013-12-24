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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.*;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * @author ali 16/12/13
 */
public class ClientConnection implements Connection, Closeable {

    private volatile boolean live = true;

    private final ILogger logger = Logger.getLogger(ClientConnection.class);

    private final ClientWriteHandler writeHandler;

    private final ClientReadHandler readHandler;

    private final ClientConnectionManagerImpl connectionManager;

    private final int connectionId;

    private final SocketChannel socketChannel;

    private Address remoteEndpoint;

    public ClientConnection(ClientConnectionManagerImpl connectionManager, IOSelector in, IOSelector out, int connectionId, SocketChannel socketChannel) {
        this.connectionManager = connectionManager;
        this.socketChannel = socketChannel;
        this.connectionId = connectionId;
        this.readHandler = new ClientReadHandler(this, in);
        this.writeHandler = new ClientWriteHandler(this, out);
    }

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

    public Address getEndPoint() {
        return remoteEndpoint;
    }

    public boolean live() {
        return live;
    }

    public long lastReadTime() {
        return readHandler.getLastHandle();
    }

    public long lastWriteTime() {
        return writeHandler.getLastHandle();
    }

    public void close() {
        close(null);
    }

    public ConnectionType getType() {
        return ConnectionType.JAVA_CLIENT;
    }

    public boolean isClient() {
        return true;
    }

    public InetAddress getInetAddress() {
        return socketChannel.socket().getInetAddress();
    }

    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress) socketChannel.socket().getRemoteSocketAddress();
    }

    public int getPort() {
        return socketChannel.socket().getPort();
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public ClientConnectionManagerImpl getConnectionManager() {
        return connectionManager;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public ClientWriteHandler getWriteHandler() {
        return writeHandler;
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
        return (InetSocketAddress) socketChannel.socket().getLocalSocketAddress();
    }

    private void innerClose() throws IOException {
        if (!live) {
            return;
        }
        live = false;
        if (socketChannel != null && socketChannel.isOpen()) {
            socketChannel.close();
        }
        readHandler.shutdown();
        writeHandler.shutdown();
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
        String message = "Connection [" + socketChannel.socket().getRemoteSocketAddress() + "] lost. Reason: ";
        if (t != null) {
            message += t.getClass().getName() + "[" + t.getMessage() + "]";
        } else {
            message += "Socket explicitly closed";
        }

        logger.info(message);
        connectionManager.destroyConnection(this);
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClientConnection)) return false;

        ClientConnection that = (ClientConnection) o;

        if (connectionId != that.connectionId) return false;

        return true;
    }

    public int hashCode() {
        return connectionId;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientConnection{");
        sb.append("live=").append(live);
        sb.append(", logger=").append(logger);
        sb.append(", writeHandler=").append(writeHandler);
        sb.append(", readHandler=").append(readHandler);
        sb.append(", connectionManager=").append(connectionManager);
        sb.append(", connectionId=").append(connectionId);
        sb.append(", socketChannel=").append(socketChannel);
        sb.append(", remoteEndpoint=").append(remoteEndpoint);
        sb.append('}');
        return sb.toString();
    }

}
