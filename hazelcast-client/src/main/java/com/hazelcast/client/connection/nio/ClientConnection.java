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

package com.hazelcast.client.connection.nio;

import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.internal.metrics.DiscardableMetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.networking.IOThreadingModel;
import com.hazelcast.internal.networking.SocketChannelWrapper;
import com.hazelcast.internal.networking.SocketConnection;
import com.hazelcast.internal.networking.SocketReader;
import com.hazelcast.internal.networking.SocketWriter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.Protocols;
import com.hazelcast.util.Clock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.StringUtil.stringToBytes;
import static com.hazelcast.util.StringUtil.timeToStringFriendly;

/**
 * Client implementation of {@link Connection}.
 * ClientConnection is a connection between a Hazelcast Client and a Hazelcast Member.
 */
public class ClientConnection implements SocketConnection, DiscardableMetricsProvider {

    @Probe
    private final int connectionId;
    private final ILogger logger;

    private final AtomicInteger pendingPacketCount = new AtomicInteger(0);
    private final SocketWriter writer;
    private final SocketReader reader;
    private final SocketChannelWrapper socketChannel;
    private final ClientConnectionManagerImpl connectionManager;
    private final LifecycleService lifecycleService;
    private final HazelcastClientInstanceImpl client;

    private volatile Address remoteEndpoint;
    private volatile boolean isHeartBeating = true;
    // the time in millis the last heartbeat was received. 0 indicates that no heartbeat has ever been received.
    private volatile long lastHeartbeatRequestedMillis;
    private volatile long lastHeartbeatReceivedMillis;
    private boolean isAuthenticatedAsOwner;
    @Probe(level = ProbeLevel.DEBUG)
    private final AtomicLong closedTime = new AtomicLong();

    private volatile Throwable closeCause;
    private volatile String closeReason;
    private int connectedServerVersion = BuildInfo.UNKNOWN_HAZELCAST_VERSION;
    private String connectedServerVersionString;

    public ClientConnection(HazelcastClientInstanceImpl client,
                            IOThreadingModel ioThreadingModel,
                            int connectionId,
                            SocketChannelWrapper socketChannel) throws IOException {
        this.client = client;
        this.connectionManager = (ClientConnectionManagerImpl) client.getConnectionManager();
        this.lifecycleService = client.getLifecycleService();
        this.socketChannel = socketChannel;
        this.connectionId = connectionId;
        this.logger = client.getLoggingService().getLogger(ClientConnection.class);
        this.reader = ioThreadingModel.newSocketReader(this);
        this.writer = ioThreadingModel.newSocketWriter(this);
    }

    public ClientConnection(HazelcastClientInstanceImpl client,
                            int connectionId) throws IOException {
        this.client = client;
        this.connectionManager = (ClientConnectionManagerImpl) client.getConnectionManager();
        this.lifecycleService = client.getLifecycleService();
        this.connectionId = connectionId;
        this.writer = null;
        this.reader = null;
        this.socketChannel = null;
        this.logger = client.getLoggingService().getLogger(ClientConnection.class);
    }

    @Override
    public void provideMetrics(MetricsRegistry registry) {
        Socket socket = socketChannel.socket();
        String connectionName = "tcp.connection["
                + socket.getLocalSocketAddress() + " -> " + socket.getRemoteSocketAddress() + "]";
        registry.scanAndRegister(this, connectionName);
        registry.scanAndRegister(reader, connectionName + ".in");
        registry.scanAndRegister(writer, connectionName + ".out");
    }

    @Override
    public void discardMetrics(MetricsRegistry registry) {
        registry.deregister(this);
        registry.deregister(reader);
        registry.deregister(writer);
    }

    @Override
    public SocketReader getSocketReader() {
        return reader;
    }

    @Override
    public SocketWriter getSocketWriter() {
        return writer;
    }

    @Override
    public SocketChannelWrapper getSocketChannel() {
        return socketChannel;
    }

    public void incrementPendingPacketCount() {
        pendingPacketCount.incrementAndGet();
    }

    public void decrementPendingPacketCount() {
        pendingPacketCount.decrementAndGet();
    }

    public int getPendingPacketCount() {
        return pendingPacketCount.get();
    }

    @Override
    public boolean write(OutboundFrame frame) {
        if (!isAlive()) {
            if (logger.isFinestEnabled()) {
                logger.finest("Connection is closed, dropping frame -> " + frame);
            }
            return false;
        }
        writer.write(frame);
        return true;
    }

    public void start() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put(stringToBytes(Protocols.CLIENT_BINARY_NEW));
        buffer.flip();
        socketChannel.write(buffer);

        // we need to give the reader a kick so it starts reading from the socket.
        reader.init();
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
        return reader.lastReadTimeMillis();
    }

    @Override
    public long lastWriteTimeMillis() {
        return writer.lastWriteTimeMillis();
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
        return socketChannel.socket().getInetAddress();
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress) socketChannel.socket().getRemoteSocketAddress();
    }

    @Override
    public int getPort() {
        return socketChannel.socket().getPort();
    }

    public ClientConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setRemoteEndpoint(Address remoteEndpoint) {
        this.remoteEndpoint = remoteEndpoint;
    }

    public InetSocketAddress getLocalSocketAddress() {
        return (InetSocketAddress) socketChannel.socket().getLocalSocketAddress();
    }

    @Override
    public void close(String reason, Throwable cause) {
        if (!closedTime.compareAndSet(0, System.currentTimeMillis())) {
            return;
        }

        closeCause = cause;
        closeReason = reason;

        String message = this + " lost. Reason: ";
        if (cause != null) {
            message += cause.getClass().getName() + '[' + cause.getMessage() + ']';
        } else {
            message += "Socket explicitly closed";
        }

        try {
            innerClose();
        } catch (Exception e) {
            logger.warning("Exception while closing connection" + e.getMessage());
        }

        if (lifecycleService.isRunning()) {
            logger.warning(message);
        } else {
            logger.finest(message);
        }

        connectionManager.onClose(this);

        client.getMetricsRegistry().discardMetrics(this);
    }

    protected void innerClose() throws IOException {
        if (socketChannel.isOpen()) {
            socketChannel.close();
        }
        reader.close();
        writer.close();
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

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "incremented in single thread")
    void onHeartbeatFailed() {
        isHeartBeating = false;
    }

    void onHeartbeatResumed() {
        isHeartBeating = true;
    }

    void onHeartbeatReceived() {
        lastHeartbeatReceivedMillis = Clock.currentTimeMillis();
    }

    void onHeartbeatRequested() {
        lastHeartbeatRequestedMillis = Clock.currentTimeMillis();
    }

    public boolean isHeartBeating() {
        return isAlive() && isHeartBeating;
    }

    public boolean isAuthenticatedAsOwner() {
        return isAuthenticatedAsOwner;
    }

    public void setIsAuthenticatedAsOwner() {
        this.isAuthenticatedAsOwner = true;
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

        if (connectionId != that.connectionId) {
            return false;
        }

        return true;
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
                + ", socketChannel=" + socketChannel
                + ", remoteEndpoint=" + remoteEndpoint
                + ", lastReadTime=" + timeToStringFriendly(lastReadTimeMillis())
                + ", lastWriteTime=" + timeToStringFriendly(lastWriteTimeMillis())
                + ", closedTime=" + timeToStringFriendly(closedTime.get())
                + ", lastHeartbeatRequested=" + timeToStringFriendly(lastHeartbeatRequestedMillis)
                + ", lastHeartbeatReceived=" + timeToStringFriendly(lastHeartbeatReceivedMillis)
                + ", connected server version=" + connectedServerVersionString
                + '}';
    }

    /**
     * Closed time is the first time connection.close called.
     *
     * @return the closed time of connection, returns zero if not closed yet
     */
    public long getClosedTime() {
        return closedTime.get();
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
