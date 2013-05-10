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
import java.util.logging.Level;

public final class TcpIpConnection implements Connection {

    private final SocketChannelWrapper socketChannel;

    private final ReadHandler readHandler;

    private final WriteHandler writeHandler;

    private final TcpIpConnectionManager connectionManager;

    private volatile boolean live = true;

    private volatile Type type = Type.NONE;

    private Address endPoint = null;

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

    public Type getType() {
        return type;
    }

    public TcpIpConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public boolean write(SocketWritable packet) {
        if (!live) {
            logger.log(Level.FINEST, "Connection is closed, won't write packet -> " + packet);
            return false;
        }
        writeHandler.enqueueSocketWritable(packet);
        return true;
    }

    public enum Type {
        NONE(false, false),
        MEMBER(true, true),
        BINARY_CLIENT(false, true),
        PROTOCOL_CLIENT(false, true),
        REST_CLIENT(false, false),
        MEMCACHE_CLIENT(false, false);

        final boolean member;
        final boolean binary;

        Type(boolean member, boolean binary) {
            this.member = member;
            this.binary = binary;
        }

        public boolean isBinary() {
            return binary;
        }

        public boolean isClient() {
            return !member;
        }
    }

    public boolean isClient() {
        return (type != null) && type != Type.NONE && type.isClient();
    }

    public void setType(Type type) {
        if (this.type == Type.NONE) {
            this.type = type;
        }
    }

    public SocketChannelWrapper getSocketChannelWrapper() {
        return socketChannel;
    }

    public InetAddress getInetAddress() {
        return socketChannel.socket().getInetAddress();
    }

    public int getPort() {
        return socketChannel.socket().getPort();
    }

    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress) socketChannel.socket().getRemoteSocketAddress();
    }

    public ReadHandler getReadHandler() {
        return readHandler;
    }

    public WriteHandler getWriteHandler() {
        return writeHandler;
    }

    public boolean live() {
        return live;
    }

    public long lastWriteTime() {
        return writeHandler.getLastHandle();
    }

    public long lastReadTime() {
        return readHandler.getLastHandle();
    }

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
        if (this == o) return true;
        if (!(o instanceof TcpIpConnection)) return false;
        TcpIpConnection that = (TcpIpConnection) o;
        return connectionId == that.getConnectionId();
    }

    @Override
    public int hashCode() {
        return connectionId;
    }

    public void close0() throws IOException {
        if (!live)
            return;
        live = false;
        if (socketChannel != null && socketChannel.isOpen()) {
            socketChannel.close();
        }
        readHandler.shutdown();
        writeHandler.shutdown();
    }

    public void close() {
        close(null);
    }

    public void close(Throwable t) {
        try {
            close0();
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
        Object connAddress = (endPoint == null) ? socketChannel.socket().getRemoteSocketAddress() : endPoint;
        String message = "Connection [" + connAddress + "] lost. Reason: ";
        if (t != null) {
            message += t.getClass().getName() + "[" + t.getMessage() + "]";
        } else {
            message += "Explicit close";
        }
        logger.log(Level.INFO, message);
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
        return "Connection [" + remoteSocketAddress + " -> " + endPoint + "] live=" + live + ", client=" + isClient() + ", type=" + type;
    }
}
