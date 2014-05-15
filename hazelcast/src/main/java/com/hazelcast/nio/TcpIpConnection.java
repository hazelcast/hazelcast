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

package com.hazelcast.nio;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.SystemLogService;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * The Tcp/Ip implementation of the {@link com.hazelcast.nio.Connection}.
 */
public final class TcpIpConnection implements Connection {

    private final SocketChannelWrapper socketChannel;

    private final ReadHandler readHandler;

    private final WriteHandler writeHandler;

    private final TcpIpConnectionManager connectionManager;

    private volatile boolean live = true;

    private volatile ConnectionType type = ConnectionType.NONE;

    private Address endPoint;

    private final ILogger logger;

    private final SystemLogService systemLogService;

    private final int connectionId;

    private ConnectionMonitor monitor;

    public TcpIpConnection(TcpIpConnectionManager connectionManager, IOSelector in, IOSelector out,
                           int connectionId, SocketChannelWrapper socketChannel) {
        this.connectionId = connectionId;
        this.logger = connectionManager.ioService.getLogger(TcpIpConnection.class.getName());
        this.systemLogService = connectionManager.ioService.getSystemLogService();
        this.connectionManager = connectionManager;
        this.socketChannel = socketChannel;
        this.readHandler = new ReadHandler(this, in);
        this.writeHandler = new WriteHandler(this, out);
    }

    public SystemLogService getSystemLogService() {
        return systemLogService;
    }

    @Override
    public ConnectionType getType() {
        return type;
    }

    public TcpIpConnectionManager getConnectionManager() {
        return connectionManager;
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

    public ReadHandler getReadHandler() {
        return readHandler;
    }

    public WriteHandler getWriteHandler() {
        return writeHandler;
    }

    @Override
    public boolean live() {
        return live;
    }

    @Override
    public long lastWriteTime() {
        return writeHandler.getLastHandle();
    }

    @Override
    public long lastReadTime() {
        return readHandler.getLastHandle();
    }

    @Override
    public Address getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(Address endPoint) {
        this.endPoint = endPoint;
    }

    public void setMonitor(ConnectionMonitor monitor) {
        this.monitor = monitor;
    }

    public ConnectionMonitor getMonitor() {
        return monitor;
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
            socketChannel.close();
        }
        readHandler.shutdown();
        writeHandler.shutdown();
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
        systemLogService.logConnection(message);
        connectionManager.destroyConnection(this);
        connectionManager.ioService.onDisconnect(endPoint);
        if (t != null && monitor != null) {
            monitor.onError(t);
        }
    }

    public int getConnectionId() {
        return connectionId;
    }

    @Override
    public String toString() {
        final Socket socket = this.socketChannel.socket();
        final SocketAddress remoteSocketAddress = socket != null ? socket.getRemoteSocketAddress() : null;
        return "Connection [" + remoteSocketAddress + " -> " + endPoint
                + "] live=" + live + ", client=" + isClient() + ", type=" + type;
    }
}
