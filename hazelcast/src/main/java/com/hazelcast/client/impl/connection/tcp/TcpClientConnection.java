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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.logging.ILogger;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLIENT_METRIC_CONNECTION_CLOSED_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLIENT_METRIC_CONNECTION_CONNECTIONID;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLIENT_METRIC_CONNECTION_EVENT_HANDLER_COUNT;
import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.StringUtil.timeToStringFriendly;

/**
 * Client implementation of {@link Connection}.
 * ClientConnection is a connection between a Hazelcast Client and a Hazelcast Member.
 */
public class TcpClientConnection implements ClientConnection {

    @Probe(name = CLIENT_METRIC_CONNECTION_CONNECTIONID, level = DEBUG)
    private final int connectionId;
    private final ILogger logger;
    private final Channel channel;
    private final TcpClientConnectionManager connectionManager;
    private final LifecycleService lifecycleService;
    private final HazelcastClientInstanceImpl client;
    private final long startTime = System.currentTimeMillis();
    private final Consumer<ClientMessage> responseHandler;
    private final ConcurrentMap attributeMap;

    @Probe(name = CLIENT_METRIC_CONNECTION_EVENT_HANDLER_COUNT, level = MANDATORY)
    private final ConcurrentMap<Long, EventHandler> eventHandlerMap = new ConcurrentHashMap<>();

    private volatile Address remoteAddress;
    @Probe(name = CLIENT_METRIC_CONNECTION_CLOSED_TIME, level = ProbeLevel.DEBUG)
    private final AtomicLong closedTime = new AtomicLong();

    private volatile Throwable closeCause;
    private volatile String closeReason;
    private String connectedServerVersion;
    private volatile UUID remoteUuid;
    private volatile UUID clusterUuid;

    public TcpClientConnection(HazelcastClientInstanceImpl client, int connectionId, Channel channel) {
        this.client = client;
        this.responseHandler = client.getInvocationService().getResponseHandler();
        this.connectionManager = (TcpClientConnectionManager) client.getConnectionManager();
        this.lifecycleService = client.getLifecycleService();
        this.channel = channel;
        this.attributeMap = channel.attributeMap();
        attributeMap.put(TcpClientConnection.class, this);
        this.connectionId = connectionId;
        this.logger = client.getLoggingService().getLogger(TcpClientConnection.class);
    }

    public TcpClientConnection(HazelcastClientInstanceImpl client, int connectionId) {
        this.client = client;
        this.responseHandler = client.getInvocationService().getResponseHandler();
        this.connectionManager = (TcpClientConnectionManager) client.getConnectionManager();
        this.lifecycleService = client.getLifecycleService();
        this.connectionId = connectionId;
        this.channel = null;
        this.attributeMap = null;
        this.logger = client.getLoggingService().getLogger(TcpClientConnection.class);
    }

    @Override
    public ConcurrentMap attributeMap() {
        return attributeMap;
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
    public void setRemoteAddress(Address remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    @Override
    public InetAddress getInetAddress() {
        return channel.socket().getInetAddress();
    }

    @Override
    public Address getRemoteAddress() {
        return remoteAddress;
    }

    public Address getInitAddress() {
        return (Address) attributeMap.get(Address.class);
    }

    @Override
    public UUID getRemoteUuid() {
        return remoteUuid;
    }

    public void setRemoteUuid(UUID remoteUuid) {
        this.remoteUuid = remoteUuid;
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
    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress) channel.remoteSocketAddress();
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

        eventHandlerMap.clear();
        try {
            innerClose();
        } catch (Exception e) {
            logger.warning("Exception while closing connection" + e.getMessage());
        }

        connectionManager.onConnectionClose(this);
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

    @Override
    public void handleClientMessage(ClientMessage message) {
        if (ClientMessage.isFlagSet(message.getHeaderFlags(), ClientMessage.BACKUP_EVENT_FLAG)) {
            responseHandler.accept(message);
        } else if (ClientMessage.isFlagSet(message.getHeaderFlags(), ClientMessage.IS_EVENT_FLAG)) {
            ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) client.getListenerService();
            listenerService.handleEventMessage(message);
        } else {
            responseHandler.accept(message);
        }
    }

    public long getStartTime() {
        return startTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TcpClientConnection)) {
            return false;
        }

        TcpClientConnection that = (TcpClientConnection) o;
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
                + ", remoteAddress=" + remoteAddress
                + ", lastReadTime=" + timeToStringFriendly(lastReadTimeMillis())
                + ", lastWriteTime=" + timeToStringFriendly(lastWriteTimeMillis())
                + ", closedTime=" + timeToStringFriendly(closedTime.get())
                + ", connected server version=" + connectedServerVersion
                + '}';
    }

    public void setConnectedServerVersion(String connectedServerVersion) {
        this.connectedServerVersion = connectedServerVersion;
    }

    @Override
    public EventHandler getEventHandler(long correlationId) {
        return eventHandlerMap.get(correlationId);
    }

    @Override
    public void removeEventHandler(long correlationId) {
        eventHandlerMap.remove(correlationId);
    }

    @Override
    public void addEventHandler(long correlationId, EventHandler handler) {
        eventHandlerMap.put(correlationId, handler);
    }

    @Override
    public void setClusterUuid(UUID uuid) {
        clusterUuid = uuid;
    }

    @Override
    public UUID getClusterUuid() {
        return clusterUuid;
    }

    // used in tests
    @Override
    public Map<Long, EventHandler> getEventHandlers() {
        return Collections.unmodifiableMap(eventHandlerMap);
    }
}
