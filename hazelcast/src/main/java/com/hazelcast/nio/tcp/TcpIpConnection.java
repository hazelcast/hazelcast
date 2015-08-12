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

package com.hazelcast.nio.tcp;

import com.hazelcast.internal.metrics.CompositeProbe;
import com.hazelcast.internal.metrics.ContainsProbes;
import com.hazelcast.internal.metrics.ProbeName;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.SocketWritable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;

/**
 * The Tcp/Ip implementation of the {@link com.hazelcast.nio.Connection}.
 *
 * A {@link TcpIpConnection} has 2 sides:
 * <ol>
 * <li>{@link ReadHandler}: the side  that takes care of reading from the other side</li>
 * <li>{@link WriteHandler}: the side that takes care of writing data to the other side</li>
 * </ol>
 */
@CompositeProbe
public final class TcpIpConnection implements Connection {

    private final SocketChannelWrapper socketChannel;

    @ContainsProbes
    private final ReadHandler readHandler;

    @ContainsProbes
    private final WriteHandler writeHandler;

    private final TcpIpConnectionManager connectionManager;

    private volatile boolean live = true;

    private volatile ConnectionType type = ConnectionType.NONE;

    private Address endPoint;

    private final ILogger logger;

    private final int connectionId;

    private TcpIpConnectionMonitor monitor;

    public TcpIpConnection(TcpIpConnectionManager connectionManager,
                           int connectionId,
                           SocketChannelWrapper socketChannel,
                           TcpIpConnectionThreadingModel threadingModel) {
        this.connectionId = connectionId;
        this.logger = connectionManager.getIoService().getLogger(TcpIpConnection.class.getName());
        this.connectionManager = connectionManager;
        this.socketChannel = socketChannel;
        this.writeHandler = threadingModel.newWriteHandler(this);
        this.readHandler = threadingModel.newReadHandler(this);
    }

     public ReadHandler getReadHandler() {
        return readHandler;
    }

    public WriteHandler getWriteHandler() {
        return writeHandler;
    }

    @Override
    public ConnectionType getType() {
        return type;
    }

    @Override
    public void setType(ConnectionType type) {
        if (this.type == ConnectionType.NONE) {
            this.type = type;
        }
    }

    public TcpIpConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public SocketChannelWrapper getSocketChannelWrapper() {
        return socketChannel;
    }

    @Override
    public InetAddress getInetAddress() {
        return socketChannel.socket().getInetAddress();
    }

    @Override
    public int getPort() {
        return socketChannel.socket().getPort();
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress) socketChannel.socket().getRemoteSocketAddress();
    }

    @Override
    public boolean isAlive() {
        return live;
    }

    @Override
    public long lastWriteTime() {
        return writeHandler.getLastWriteTime();
    }

    @Override
    public long lastReadTime() {
        return readHandler.getLastReadTime();
    }

    @Override
    public Address getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(Address endPoint) {
        this.endPoint = endPoint;
    }

    public void setMonitor(TcpIpConnectionMonitor monitor) {
        this.monitor = monitor;
    }

    public TcpIpConnectionMonitor getMonitor() {
        return monitor;
    }

    public int getConnectionId() {
        return connectionId;
    }

    Object getConnectionAddress() {
        return (endPoint == null) ? socketChannel.socket().getRemoteSocketAddress() : endPoint;
    }

    @ProbeName
    private String getProbeName() {
        Socket socket = socketChannel.socket();
        SocketAddress localSocketAddress = socket != null ? socket.getLocalSocketAddress() : null;
        SocketAddress remoteSocketAddress = socket != null ? socket.getRemoteSocketAddress() : null;
        String id = getType() + "#" + localSocketAddress + "->" + remoteSocketAddress;
        return "connection[" + id + "]";
    }

    public void setSendBufferSize(int size) throws SocketException {
        socketChannel.socket().setSendBufferSize(size);
    }

    public void setReceiveBufferSize(int size) throws SocketException {
        socketChannel.socket().setReceiveBufferSize(size);
    }

    @Override
    public boolean isClient() {
        final ConnectionType t = type;
        return (t != null) && t != ConnectionType.NONE && t.isClient();
    }

    /**
     * Starts this connection.
     *
     * Starting means that the connection is going to register itself to listen to incoming traffic.
     */
    public void start() {
        writeHandler.start();
        readHandler.start();
    }

    @Override
    public boolean write(SocketWritable packet) {
        if (!live) {
            if (logger.isFinestEnabled()) {
                logger.finest("Connection is closed, won't write packet -> " + packet);
            }
            return false;
        }
        writeHandler.offer(packet);
        return true;
    }

   @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TcpIpConnection)) {
            return false;
        }
        TcpIpConnection that = (TcpIpConnection) o;
        return connectionId == that.getConnectionId();
    }

    @Override
    public int hashCode() {
        return connectionId;
    }

    private void close0() throws IOException {
        if (!live) {
            return;
        }
        live = false;

        if (socketChannel != null && socketChannel.isOpen()) {
            readHandler.shutdown();
            writeHandler.shutdown();
            socketChannel.close();
        }
    }

    @Override
    public void close() {
        close(null);
    }

    public void close(Throwable t) {
        if (!live) {
            return;
        }
        try {
            close0();
        } catch (Exception e) {
            logger.warning(e);
        }

        Object connAddress = getConnectionAddress();
        String message = "Connection [" + connAddress + "] lost. Reason: ";
        if (t != null) {
            message += t.getClass().getName() + "[" + t.getMessage() + "]";
        } else {
            message += "Socket explicitly closed";
        }

        logger.info(message);
        connectionManager.destroyConnection(this);
        connectionManager.getIoService().onDisconnect(endPoint);
        if (t != null && monitor != null) {
            monitor.onError(t);
        }
    }

    @Override
    public String toString() {
        Socket socket = socketChannel.socket();
        SocketAddress localSocketAddress = socket != null ? socket.getLocalSocketAddress() : null;
        SocketAddress remoteSocketAddress = socket != null ? socket.getRemoteSocketAddress() : null;
        return "Connection [" + localSocketAddress + " -> " + remoteSocketAddress
                + "], endpoint=" + endPoint + ", live=" + live + ", type=" + type;
    }
}
