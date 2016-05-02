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

package com.hazelcast.nio.tcp;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.OutboundFrame;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Tcp/Ip implementation of the {@link com.hazelcast.nio.Connection}.
 *
 * A {@link TcpIpConnection} is not responsible for reading or writing data to a socket, this is done through:
 * <ol>
 * <li>{@link SocketReader}: which care of reading from the socket and feeding it into the system/li>
 * <li>{@link SocketWriter}: which care of writing data to the socket.</li>
 * </ol>
 *
 * @see IOThreadingModel
 */
public final class TcpIpConnection implements Connection {

    private final SocketChannelWrapper socketChannel;

    private final SocketReader socketReader;

    private final SocketWriter socketWriter;

    private final TcpIpConnectionManager connectionManager;

    private final AtomicBoolean alive = new AtomicBoolean(true);

    private final ILogger logger;

    private final int connectionId;

    private Address endPoint;

    private TcpIpConnectionMonitor monitor;

    private volatile ConnectionType type = ConnectionType.NONE;

    public TcpIpConnection(TcpIpConnectionManager connectionManager,
                           int connectionId,
                           SocketChannelWrapper socketChannel,
                           IOThreadingModel ioThreadingModel) {
        this.connectionId = connectionId;
        this.logger = connectionManager.getIoService().getLogger(TcpIpConnection.class.getName());
        this.connectionManager = connectionManager;
        this.socketChannel = socketChannel;
        this.socketWriter = ioThreadingModel.newSocketWriter(this);
        this.socketReader = ioThreadingModel.newSocketReader(this);
    }

    public SocketReader getSocketReader() {
        return socketReader;
    }

    public SocketWriter getSocketWriter() {
        return socketWriter;
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
        return alive.get();
    }

    @Override
    public long lastWriteTimeMillis() {
        return socketWriter.getLastWriteTimeMillis();
    }

    @Override
    public long lastReadTimeMillis() {
        return socketReader.getLastReadTimeMillis();
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

    public Object getMetricsId() {
        Socket socket = socketChannel.socket();
        SocketAddress localSocketAddress = socket != null ? socket.getLocalSocketAddress() : null;
        SocketAddress remoteSocketAddress = socket != null ? socket.getRemoteSocketAddress() : null;
        return getType() + "#" + localSocketAddress + "->" + remoteSocketAddress;
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
        socketReader.init();
    }

    @Override
    public boolean write(OutboundFrame frame) {
        if (!alive.get()) {
            if (logger.isFinestEnabled()) {
                logger.finest("Connection is closed, won't write packet -> " + frame);
            }
            return false;
        }
        socketWriter.write(frame);
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

    @Override
    public void close() {
        close(null);
    }

    public void close(Throwable t) {
        if (!alive.compareAndSet(true, false)) {
            // it is already closed.
            return;
        }

        try {
            if (socketChannel != null && socketChannel.isOpen()) {
                socketReader.close();
                socketWriter.close();
                socketChannel.close();
            }
        } catch (Exception e) {
            logger.warning(e);
        }

        Object connAddress = getConnectionAddress();
        String message = "Connection [" + connAddress + "] lost. Reason: ";
        if (t == null) {
            message += "Socket explicitly closed";
        } else {
            message += t.getClass().getName() + "[" + t.getMessage() + "]";
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
                + "], endpoint=" + endPoint + ", alive=" + alive + ", type=" + type;
    }
}
