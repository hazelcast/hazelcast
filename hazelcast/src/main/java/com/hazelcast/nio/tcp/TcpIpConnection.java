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

package com.hazelcast.nio.tcp;

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

/**
 * The Tcp/Ip implementation of the {@link com.hazelcast.nio.Connection}.
 * <p/>
 * A Connection has 2 sides:
 * <ol>
 * <li>the side where it receives data from the remote  machine</li>
 * <li>the side where it sends data to the remote machine</li>
 * </ol>
 * <p/>
 * The reading side is the {@link TcpIpConnectionReadHandler} and the writing side of this connection
 * is the {@link TcpIpConnectionWriteHandler}.
 */
public final class TcpIpConnection implements Connection {

    private final ILogger logger;
    private final int connectionId;
    private final SocketChannelWrapper socketChannel;
    private final TcpIpConnectionReadHandler readHandler;
    private final TcpIpConnectionWriteHandler writeHandler;
    private final TcpIpConnectionManager connectionManager;
    private volatile boolean live = true;
    private volatile ConnectionType type = ConnectionType.NONE;
    private Address endPoint;
    private TcpIpConnectionMonitor monitor;

    public TcpIpConnection(TcpIpConnectionManager connectionManager, IOReactor in, IOReactor out,
                           int connectionId, SocketChannelWrapper socketChannel) {
        this.connectionId = connectionId;
        this.logger = connectionManager.ioService.getLogger(TcpIpConnection.class.getName());
        this.connectionManager = connectionManager;
        this.socketChannel = socketChannel;
        this.readHandler = new TcpIpConnectionReadHandler(this, in);
        this.writeHandler = new TcpIpConnectionWriteHandler(this, out);
    }

    @Override
    public ConnectionType getType() {
        return type;
    }

    public TcpIpConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public int getConnectionId() {
        return connectionId;
    }

    @Override
    public boolean isAlive() {
        return live;
    }

    @Override
    public long lastWriteTime() {
        return writeHandler.lastWriteTime();
    }

    @Override
    public long lastReadTime() {
        return readHandler.lastReadTime();
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

    @Override
    public boolean isClient() {
        final ConnectionType t = type;
        return (t != null) && t != ConnectionType.NONE && t.isClient();
    }

    public void setType(ConnectionType type) {
        if (this.type == ConnectionType.NONE) {
            this.type = type;
        }
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

    public TcpIpConnectionReadHandler getReadHandler() {
        return readHandler;
    }

    public TcpIpConnectionWriteHandler getWriteHandler() {
        return writeHandler;
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
        Object connAddress = (endPoint == null) ? socketChannel.socket().getRemoteSocketAddress() : endPoint;
        String message = "Connection [" + connAddress + "] lost. Reason: ";
        if (t != null) {
            message += t.getClass().getName() + "[" + t.getMessage() + "]";
        } else {
            message += "Socket explicitly closed";
        }

        logger.info(message);
        connectionManager.destroyConnection(this);
        connectionManager.ioService.onDisconnect(endPoint);
        if (t != null && monitor != null) {
            monitor.onError(t);
        }
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

    public void dumpPerformanceMetrics(StringBuffer sb) {
        Socket socket = this.socketChannel.socket();
        SocketAddress localSocketAddress = socket != null ? socket.getLocalSocketAddress() : null;
        SocketAddress remoteSocketAddress = socket != null ? socket.getRemoteSocketAddress() : null;

        sb.append("Connection [").append(localSocketAddress).append(" -> ").append(remoteSocketAddress).append(']');
        sb.append(" write.handleCount=").append(writeHandler.getHandleCount());
        sb.append(" write.unscheduledCount=").append(writeHandler.getUnscheduledCount());
        sb.append(" write.packetCount=").append(writeHandler.getPacketCount());
        double packetPerHandleRatio = writeHandler.getPacketCount() * 1d / writeHandler.getHandleCount();
        sb.append(" write.packet/handle=").append(packetPerHandleRatio);
        sb.append('\n');
    }

    @Override
    public String toString() {
        Socket socket = this.socketChannel.socket();
        SocketAddress localSocketAddress = socket != null ? socket.getLocalSocketAddress() : null;
        SocketAddress remoteSocketAddress = socket != null ? socket.getRemoteSocketAddress() : null;
        return "Connection [" + localSocketAddress + " -> " + remoteSocketAddress
                + "], endpoint=" + endPoint + ", live=" + live + ", type=" + type;
    }

}
