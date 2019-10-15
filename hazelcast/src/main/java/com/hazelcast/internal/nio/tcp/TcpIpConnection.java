/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nio.tcp;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionLifecycleListener;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.nio.IOService;

import java.io.EOFException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.security.cert.Certificate;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static com.hazelcast.internal.nio.ConnectionType.MEMBER;
import static com.hazelcast.internal.nio.ConnectionType.NONE;

/**
 * The Tcp/Ip implementation of the {@link Connection}.
 * <p>
 * A {@link TcpIpConnection} is not responsible for reading or writing data to the socket; that is task of
 * the {@link Channel}.
 *
 * @see Networking
 */
@SuppressWarnings("checkstyle:methodcount")
public class TcpIpConnection
        implements Connection {

    private final Channel channel;
    private final ConcurrentMap attributeMap;

    private final TcpIpEndpointManager endpointManager;

    private final AtomicBoolean alive = new AtomicBoolean(true);

    // indicate whether connection bind exchange is in progress/done (true) or not yet initiated (when false)
    private final AtomicBoolean binding = new AtomicBoolean();

    private final ILogger logger;

    private final int connectionId;

    private final IOService ioService;

    private Address endPoint;

    private TcpIpConnectionErrorHandler errorHandler;

    private volatile ConnectionType type = NONE;

    private volatile ConnectionLifecycleListener lifecycleListener;

    private volatile Throwable closeCause;

    private volatile String closeReason;


    public TcpIpConnection(TcpIpEndpointManager endpointManager,
                           ConnectionLifecycleListener lifecycleListener,
                           int connectionId,
                           Channel channel) {
        this.connectionId = connectionId;
        this.endpointManager = endpointManager;
        this.lifecycleListener = lifecycleListener;
        this.ioService = endpointManager.getNetworkingService().getIoService();
        this.logger = ioService.getLoggingService().getLogger(TcpIpConnection.class);
        this.channel = channel;
        this.attributeMap = channel.attributeMap();
        attributeMap.put(TcpIpConnection.class, this);
    }

    public Channel getChannel() {
        return channel;
    }

    @Override
    public ConnectionType getType() {
        return type;
    }

    @Override
    public void setType(ConnectionType type) {
        if (this.type != NONE) {
            return;
        }

        this.type = type;
        if (type == MEMBER) {
            logger.info("Initialized new cluster connection between "
                        + channel.localSocketAddress() + " and " + channel.remoteSocketAddress());
        }
    }

    @Probe
    private int getConnectionType() {
        ConnectionType t = type;
        return t == null ? -1 : t.ordinal();
    }

    public TcpIpEndpointManager getEndpointManager() {
        return endpointManager;
    }

    @Override
    public InetAddress getInetAddress() {
        return channel.socket().getInetAddress();
    }

    @Override
    public int getPort() {
        return channel.socket().getPort();
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress) channel.remoteSocketAddress();
    }

    @Override
    public boolean isAlive() {
        return alive.get();
    }

    @Override
    public long lastWriteTimeMillis() {
        return channel.lastWriteTimeMillis();
    }

    @Override
    public long lastReadTimeMillis() {
        return channel.lastReadTimeMillis();
    }

    @Override
    public Address getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(Address endPoint) {
        this.endPoint = endPoint;
    }

    public void setErrorHandler(TcpIpConnectionErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    public int getConnectionId() {
        return connectionId;
    }

    @Override
    public boolean isClient() {
        ConnectionType t = type;
        return t != null && t != NONE && t.isClient();
    }

    @Override
    public boolean write(OutboundFrame frame) {
        if (channel.write(frame)) {
            return true;
        }

        if (logger.isFinestEnabled()) {
            logger.finest("Connection is closed, won't write packet -> " + frame);
        }
        return false;
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
            channel.close();
        } catch (Exception e) {
            logger.warning(e);
        }

        lifecycleListener.onConnectionClose(this, null, false);
        ioService.onDisconnect(endPoint, cause);
        if (cause != null && errorHandler != null) {
            errorHandler.onError(cause);
        }
    }

    public boolean setBinding() {
        return binding.compareAndSet(false, true);
    }

    private void logClose() {
        Level logLevel = resolveLogLevelOnClose();
        if (!logger.isLoggable(logLevel)) {
            return;
        }

        String message = toString() + " closed. Reason: ";
        if (closeReason != null) {
            message += closeReason;
        } else if (closeCause != null) {
            message += closeCause.getClass().getName() + "[" + closeCause.getMessage() + "]";
        } else {
            message += "Socket explicitly closed";
        }

        if (Level.FINEST.equals(logLevel)) {
            logger.log(logLevel, message, closeCause);
        } else if (closeCause == null || closeCause instanceof EOFException || closeCause instanceof CancelledKeyException) {
            logger.log(logLevel, message);
        } else {
            logger.log(logLevel, message, closeCause);
        }
    }

    private Level resolveLogLevelOnClose() {
        if (!ioService.isActive()) {
            return Level.FINEST;
        }

        if (closeCause == null || closeCause instanceof EOFException || closeCause instanceof CancelledKeyException) {
            if (type == ConnectionType.REST_CLIENT || type == ConnectionType.MEMCACHE_CLIENT) {
                // text-based clients are expected to come and go frequently.
                return Level.FINE;
            } else {
                return Level.INFO;
            }
        } else {
            return Level.WARNING;
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
        return "Connection[id=" + connectionId
                + ", " + channel.localSocketAddress() + "->" + channel.remoteSocketAddress()
                + ", qualifier=" + endpointManager.getEndpointQualifier()
                + ", endpoint=" + endPoint
                + ", alive=" + alive
                + ", type=" + type
                + "]";
    }

    @Override
    public Certificate[] getRemoteCertificates() {
        return attributeMap != null ? (Certificate[]) attributeMap.get(Certificate.class) : null;
    }
}
