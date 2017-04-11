/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.DiscardableMetricsProvider;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.IOThreadingModel;
import com.hazelcast.internal.networking.SocketChannelWrapper;
import com.hazelcast.internal.networking.SocketConnection;
import com.hazelcast.internal.networking.SocketReader;
import com.hazelcast.internal.networking.SocketWriter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.OutboundFrame;

import java.io.EOFException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.CancelledKeyException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Tcp/Ip implementation of the {@link com.hazelcast.nio.Connection}.
 * <p>
 * A {@link TcpIpConnection} is not responsible for reading or writing data to a socket, this is done through:
 * <ol>
 * <li>{@link SocketReader}: which care of reading from the socket and feeding it into the system/li>
 * <li>{@link SocketWriter}: which care of writing data to the socket.</li>
 * </ol>
 *
 * @see IOThreadingModel
 */
@SuppressWarnings("checkstyle:methodcount")
public final class TcpIpConnection implements SocketConnection, MetricsProvider, DiscardableMetricsProvider {

    private final SocketChannelWrapper socketChannel;

    private final SocketReader socketReader;

    private final SocketWriter socketWriter;

    private final TcpIpConnectionManager connectionManager;

    private final AtomicBoolean alive = new AtomicBoolean(true);

    private final ILogger logger;

    private final int connectionId;

    private final IOService ioService;

    private Address endPoint;

    private TcpIpConnectionMonitor monitor;

    private volatile ConnectionType type = ConnectionType.NONE;

    private volatile Throwable closeCause;

    private volatile String closeReason;

    public TcpIpConnection(TcpIpConnectionManager connectionManager,
                           int connectionId,
                           SocketChannelWrapper socketChannel,
                           IOThreadingModel ioThreadingModel) {
        this.connectionId = connectionId;
        this.connectionManager = connectionManager;
        this.ioService = connectionManager.getIoService();
        this.logger = ioService.getLoggingService().getLogger(TcpIpConnection.class);
        this.socketChannel = socketChannel;
        this.socketWriter = ioThreadingModel.newSocketWriter(this);
        this.socketReader = ioThreadingModel.newSocketReader(this);
    }

    @Override
    public void provideMetrics(MetricsRegistry registry) {
        Socket socket = socketChannel.socket();
        SocketAddress localSocketAddress = socket != null ? socket.getLocalSocketAddress() : null;
        SocketAddress remoteSocketAddress = socket != null ? socket.getRemoteSocketAddress() : null;
        String metricsId = localSocketAddress + "->" + remoteSocketAddress;
        registry.scanAndRegister(socketWriter, "tcp.connection[" + metricsId + "].out");
        registry.scanAndRegister(socketReader, "tcp.connection[" + metricsId + "].in");
    }

    @Override
    public void discardMetrics(MetricsRegistry registry) {
        registry.deregister(socketReader);
        registry.deregister(socketWriter);
    }

    public SocketReader getSocketReader() {
        return socketReader;
    }

    public SocketWriter getSocketWriter() {
        return socketWriter;
    }

    @Override
    public SocketChannelWrapper getSocketChannel() {
        return socketChannel;
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

    @Probe
    private int getConnectionType() {
        ConnectionType t = type;
        return t == null ? -1 : t.ordinal();
    }

    public TcpIpConnectionManager getConnectionManager() {
        return connectionManager;
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
        return socketWriter.lastWriteTimeMillis();
    }

    @Override
    public long lastReadTimeMillis() {
        return socketReader.lastReadTimeMillis();
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

    public void setSendBufferSize(int size) throws SocketException {
        socketChannel.socket().setSendBufferSize(size);
    }

    public void setReceiveBufferSize(int size) throws SocketException {
        socketChannel.socket().setReceiveBufferSize(size);
    }

    @Override
    public boolean isClient() {
        ConnectionType t = type;
        return (t != null) && t != ConnectionType.NONE && t.isClient();
    }


    /**
     * Starts this connection.
     * <p>
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
    public void close(String reason, Throwable cause) {
        if (!alive.compareAndSet(true, false)) {
            // it is already closed.
            return;
        }

        this.closeCause = cause;
        this.closeReason = reason;

        logClose();

        try {
            if (socketChannel != null && socketChannel.isOpen()) {
                socketReader.close();
                socketWriter.close();
                socketChannel.close();
            }
        } catch (Exception e) {
            logger.warning(e);
        }

        connectionManager.onConnectionClose(this);
        ioService.onDisconnect(endPoint, cause);
        if (cause != null && monitor != null) {
            monitor.onError(cause);
        }
    }

    private void logClose() {
        String message = toString() + " closed. Reason: ";
        if (closeReason != null) {
            message += closeReason;
        } else if (closeCause != null) {
            message += closeCause.getClass().getName() + "[" + closeCause.getMessage() + "]";
        } else {
            message += "Socket explicitly closed";
        }

        if (ioService.isActive()) {
            if (closeCause == null || closeCause instanceof EOFException || closeCause instanceof CancelledKeyException) {
                logger.info(message);
            } else {
                logger.warning(message, closeCause);
            }
        } else {
            if (closeCause == null) {
                logger.finest(message);
            } else {
                logger.finest(message, closeCause);
            }
        }
    }

    @Override
    public Throwable getCloseCause() {
        return closeCause;
    }

    @Override
    public String getCloseReason() {
        if (closeReason == null) {
            return closeCause == null ? null : closeCause.getMessage();
        } else {
            return closeReason;
        }
    }

    @Override
    public String toString() {
        Socket socket = socketChannel.socket();
        SocketAddress localSocketAddress = socket != null ? socket.getLocalSocketAddress() : null;
        SocketAddress remoteSocketAddress = socket != null ? socket.getRemoteSocketAddress() : null;
        return "Connection[id=" + connectionId
                + ", " + localSocketAddress + "->" + remoteSocketAddress
                + ", endpoint=" + endPoint
                + ", alive=" + alive
                + ", type=" + type
                + "]";
    }
}
