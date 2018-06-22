/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientResponseHandler;
import com.hazelcast.client.spi.impl.listener.AbstractClientListenerService;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.StringUtil.timeToStringFriendly;

/**
 * Client implementation of {@link Connection}.
 * ClientConnection is a connection between a Hazelcast Client and a Hazelcast Member.
 */
public class ClientConnection implements Connection {

    @Probe
    private final int connectionId;
    private final ILogger logger;
    private final Channel channel;
    private final ClientConnectionManagerImpl connectionManager;
    private final LifecycleService lifecycleService;
    private final HazelcastClientInstanceImpl client;
    private final long startTime = System.currentTimeMillis();
    private final ClientResponseHandler responseHandler;

    private volatile Address remoteEndpoint;
    private volatile boolean isAuthenticatedAsOwner;
    @Probe(level = ProbeLevel.DEBUG)
    private final AtomicLong closedTime = new AtomicLong();

    private volatile Throwable closeCause;
    private volatile String closeReason;
    private int connectedServerVersion = BuildInfo.UNKNOWN_HAZELCAST_VERSION;
    private String connectedServerVersionString;

    public ClientConnection(HazelcastClientInstanceImpl client, int connectionId, Channel channel) {
        this.client = client;
        this.responseHandler = client.getInvocationService().getResponseHandler();
        this.connectionManager = (ClientConnectionManagerImpl) client.getConnectionManager();
        this.lifecycleService = client.getLifecycleService();
        this.channel = channel;
        channel.attributeMap().put(ClientConnection.class, this);
        this.connectionId = connectionId;
        this.logger = client.getLoggingService().getLogger(ClientConnection.class);
    }

    public ClientConnection(HazelcastClientInstanceImpl client, int connectionId) {
        this.client = client;
        this.responseHandler = client.getInvocationService().getResponseHandler();
        this.connectionManager = (ClientConnectionManagerImpl) client.getConnectionManager();
        this.lifecycleService = client.getLifecycleService();
        this.connectionId = connectionId;
        this.channel = null;
        this.logger = client.getLoggingService().getLogger(ClientConnection.class);
    }

    @Override
    public boolean write(OutboundFrame frame) {
        if (channel.write(frame)) {
            return true;
        }

        if (logger.isFinestEnabled()) {
            logger.finest("Connection is closed, dropping frame -> " + frame);
        }
        return false;
    }

    @Override
    public Address getEndPoint() {
        return remoteEndpoint;
    }

    @Override
    public boolean isAlive() {
        return closedTime.get() == 0;
    }

    @Override
    public long lastReadTimeMillis() {
        return channel.lastReadTimeMillis();
    }

    @Override
    public long lastWriteTimeMillis() {
        return channel.lastWriteTimeMillis();
    }

    @Override
    public void setType(ConnectionType type) {
        //NO OP
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
        return channel.socket().getInetAddress();
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress) channel.remoteSocketAddress();
    }

    @Override
    public int getPort() {
        return channel.socket().getPort();
    }

    public ClientConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setRemoteEndpoint(Address remoteEndpoint) {
        this.remoteEndpoint = remoteEndpoint;
    }

    public InetSocketAddress getLocalSocketAddress() {
        return (InetSocketAddress) channel.localSocketAddress();
    }

    @Override
    public void close(String reason, Throwable cause) {
        if (!closedTime.compareAndSet(0, System.currentTimeMillis())) {
            return;
        }

        closeCause = cause;
        closeReason = reason;

        logClose();

        try {
            innerClose();
        } catch (Exception e) {
            logger.warning("Exception while closing connection" + e.getMessage());
        }

        connectionManager.onClose(this);

        client.getMetricsRegistry().discardMetrics(this);
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

        if (lifecycleService.isRunning()) {
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

    protected void innerClose() throws IOException {
        channel.close();
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

    public void handleClientMessage(ClientMessage message) {
        if (message.isFlagSet(ClientMessage.LISTENER_EVENT_FLAG)) {
            AbstractClientListenerService listenerService = (AbstractClientListenerService) client.getListenerService();
            listenerService.handleClientMessage(message);
        } else {
            responseHandler.handle(message);
        }
    }

    public boolean isAuthenticatedAsOwner() {
        return isAuthenticatedAsOwner;
    }

    public void setIsAuthenticatedAsOwner() {
        this.isAuthenticatedAsOwner = true;
    }

    public long getStartTime() {
        return startTime;
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
        return connectionId == that.connectionId;
    }

    @Override
    public int hashCode() {
        return connectionId;
    }

    @Override
    public String toString() {
        return "ClientConnection{"
                + "alive=" + isAlive()
                + ", connectionId=" + connectionId
                + ", channel=" + channel
                + ", remoteEndpoint=" + remoteEndpoint
                + ", lastReadTime=" + timeToStringFriendly(lastReadTimeMillis())
                + ", lastWriteTime=" + timeToStringFriendly(lastWriteTimeMillis())
                + ", closedTime=" + timeToStringFriendly(closedTime.get())
                + ", connected server version=" + connectedServerVersionString
                + '}';
    }

    public void setConnectedServerVersion(String connectedServerVersion) {
        this.connectedServerVersionString = connectedServerVersion;
        this.connectedServerVersion = BuildInfo.calculateVersion(connectedServerVersion);
    }

    public int getConnectedServerVersion() {
        return connectedServerVersion;
    }

    public String getConnectedServerVersionString() {
        return connectedServerVersionString;
    }
}
