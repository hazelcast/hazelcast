/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.auditlog.AuditlogTypeIds;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionLifecycleListener;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.logging.ILogger;

import java.io.EOFException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_CONNECTION_CONNECTION_TYPE;
import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.metrics.ProbeUnit.ENUM;
import static com.hazelcast.internal.nio.ConnectionType.MEMBER;
import static com.hazelcast.internal.nio.ConnectionType.NONE;

/**
 * The Tcp/Ip implementation of the {@link Connection}.
 * <p>
 * A {@link TcpServerConnection} is not responsible for reading or writing data to the socket; that is task of
 * the {@link Channel}.
 *
 * @see Networking
 */
@SuppressWarnings("checkstyle:methodcount")
public class TcpServerConnection implements ServerConnection {

    private final Channel channel;
    private final ConcurrentMap attributeMap;

    private final TcpServerConnectionManager connectionManager;

    private final AtomicBoolean alive = new AtomicBoolean(true);

    // indicate whether connection handshake is in progress/done (true) or not yet initiated (when false)
    private final AtomicBoolean handshake = new AtomicBoolean();

    private final ILogger logger;

    // Flag that indicates if the connection is accepted on this member (server-side)
    // See also TcpServerAcceptor and TcpServerConnector
    private final boolean acceptorSide;

    private final int connectionId;

    private final ServerContext serverContext;

    private Address remoteAddress;

    private UUID remoteUuid;

    private TcpServerConnectionErrorHandler errorHandler;

    private volatile String connectionType = NONE;

    private volatile ConnectionLifecycleListener<TcpServerConnection> lifecycleListener;

    private volatile Throwable closeCause;

    private volatile String closeReason;
    private volatile int planeIndex = -1;

    public TcpServerConnection(TcpServerConnectionManager connectionManager,
                               ConnectionLifecycleListener<TcpServerConnection> lifecycleListener,
                               int connectionId,
                               Channel channel,
                               boolean acceptorSide
    ) {
        this.connectionId = connectionId;
        this.connectionManager = connectionManager;
        this.lifecycleListener = lifecycleListener;
        this.serverContext = connectionManager.getServer().getContext();
        this.logger = serverContext.getLoggingService().getLogger(TcpServerConnection.class);
        this.channel = channel;
        this.acceptorSide = acceptorSide;
        this.attributeMap = channel.attributeMap();
        attributeMap.put(ServerConnection.class, this);
    }

    @Override
    public ConcurrentMap attributeMap() {
        return attributeMap;
    }

    public Channel getChannel() {
        return channel;
    }

    public int getPlaneIndex() {
        return planeIndex;
    }

    public void setPlaneIndex(int planeIndex) {
        this.planeIndex = planeIndex;
    }

    @Override
    public String getConnectionType() {
        return connectionType;
    }

    @Probe(name = TCP_METRIC_CONNECTION_CONNECTION_TYPE, unit = ENUM, level = DEBUG)
    private int getType() {
        return ConnectionType.getTypeId(connectionType);
    }

    @Override
    public void setConnectionType(String connectionType) {
        Objects.requireNonNull(connectionType);
        if (!this.connectionType.equals(NONE)) {
            return;
        }

        this.connectionType = connectionType;
        if (connectionType.equals(MEMBER)) {
            logger.info("Initialized new cluster connection between "
                    + channel.localSocketAddress() + " and " + channel.remoteSocketAddress());
        }
    }

    public TcpServerConnectionManager getConnectionManager() {
        return connectionManager;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress) channel.remoteSocketAddress();
    }

    @Override
    public InetAddress getInetAddress() {
        return channel.socket().getInetAddress();
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
    public Address getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public void setRemoteAddress(Address remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    @Override
    public UUID getRemoteUuid() {
        return remoteUuid;
    }

    @Override
    public void setRemoteUuid(UUID remoteUuid) {
        this.remoteUuid = remoteUuid;
    }

    public boolean isAcceptorSide() {
        return acceptorSide;
    }

    public void setErrorHandler(TcpServerConnectionErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    public int getConnectionId() {
        return connectionId;
    }

    @Override
    public boolean isClient() {
        return !connectionType.equals(MEMBER);
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
        if (!(o instanceof TcpServerConnection)) {
            return false;
        }
        TcpServerConnection that = (TcpServerConnection) o;
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

        serverContext.getAuditLogService()
            .eventBuilder(AuditlogTypeIds.NETWORK_DISCONNECT)
            .message("Closing server connection.")
            .addParameter("reason", reason)
            .addParameter("cause", cause)
            .addParameter("remoteAddress", remoteAddress)
            .addParameter("remoteUuid", remoteUuid)
            .log();

        logClose();

        try {
            channel.close();
        } catch (Exception e) {
            logger.warning(e);
        }

        lifecycleListener.onConnectionClose(this, cause, false);
        serverContext.onDisconnect(remoteAddress, cause);

        LoginContext lc = (LoginContext) attributeMap.remove(LoginContext.class);
        if (lc != null) {
            try {
                lc.logout();
            } catch (LoginException e) {
                logger.warning("Logout failed", e);
            }
        }
        if (cause != null && errorHandler != null) {
            errorHandler.onError(cause);
        }
    }

    public boolean setHandshake() {
        return handshake.compareAndSet(false, true);
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
        if (!serverContext.isNodeActive()) {
            return Level.FINEST;
        }

        if (closeCause == null || closeCause instanceof EOFException || closeCause instanceof CancelledKeyException) {
            if (connectionType.equals(ConnectionType.REST_CLIENT) || connectionType.equals(ConnectionType.MEMCACHE_CLIENT)) {
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
                + ", qualifier=" + connectionManager.getEndpointQualifier()
                + ", endpoint=" + remoteAddress
                + ", remoteUuid=" + remoteUuid
                + ", alive=" + alive
                + ", connectionType=" + connectionType
                + ", planeIndex=" + planeIndex
                + "]";
    }
}
