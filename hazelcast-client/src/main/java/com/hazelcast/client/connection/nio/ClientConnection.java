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

package com.hazelcast.client.connection.nio;

import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.Member;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThread;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.Clock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.StringUtil.stringToBytes;
import static com.hazelcast.util.StringUtil.timeToStringFriendly;

/**
 * Client implementation of {@link Connection}.
 * ClientConnection is a connection between a Hazelcast Client and a Hazelcast Member.
 */
public class ClientConnection implements Connection {

    @Probe
    protected final int connectionId;
    private final AtomicBoolean live = new AtomicBoolean(true);
    private final ILogger logger;

    private final AtomicInteger pendingPacketCount = new AtomicInteger(0);
    private final ClientWriteHandler writeHandler;
    private final ClientReadHandler readHandler;
    private final SocketChannelWrapper socketChannelWrapper;
    private final ClientConnectionManager connectionManager;
    private final LifecycleService lifecycleService;
    private final HazelcastClientInstanceImpl client;

    private volatile Address remoteEndpoint;
    private volatile boolean isHeartBeating = true;
    // the time in millis the last heartbeat was received. 0 indicates that no heartbeat has ever been received.
    private volatile long lastHeartbeatRequestedMillis;
    private volatile long lastHeartbeatReceivedMillis;
    private boolean isAuthenticatedAsOwner;
    @Probe(level = ProbeLevel.DEBUG)
    private volatile long closedTime;

    private volatile Throwable closeCause;
    private volatile String closeReason;
    private int connectedServerVersion = BuildInfo.UNKNOWN_HAZELCAST_VERSION;
    private String connectedServerVersionString;

    /**
     * The list of members from which the client resources are deleted at the time of connection authentication. The client may
     * use this list to re-register the client resources such the listeners to these members.
     */
    private List<Member> clientUnregisteredMembers;

    public ClientConnection(HazelcastClientInstanceImpl client, NonBlockingIOThread in, NonBlockingIOThread out,
                            int connectionId, SocketChannelWrapper socketChannelWrapper) throws IOException {
        final Socket socket = socketChannelWrapper.socket();

        this.client = client;
        this.connectionManager = client.getConnectionManager();
        this.lifecycleService = client.getLifecycleService();
        this.socketChannelWrapper = socketChannelWrapper;
        this.connectionId = connectionId;
        LoggingService clientLoggingService = client.getLoggingService();
        this.logger = clientLoggingService.getLogger(ClientConnection.class);
        boolean directBuffer = client.getProperties().getBoolean(GroupProperty.SOCKET_CLIENT_BUFFER_DIRECT);
        this.readHandler = new ClientReadHandler(this, in, socket.getReceiveBufferSize(), directBuffer, clientLoggingService);
        this.writeHandler = new ClientWriteHandler(this, out, socket.getSendBufferSize(), directBuffer, clientLoggingService);

        MetricsRegistryImpl metricsRegistry = client.getMetricsRegistry();
        String connectionName = "tcp.connection["
                + socket.getLocalSocketAddress() + " -> " + socket.getRemoteSocketAddress() + "]";
        metricsRegistry.scanAndRegister(this, connectionName);
        metricsRegistry.scanAndRegister(readHandler, connectionName + ".in");
        metricsRegistry.scanAndRegister(writeHandler, connectionName + ".out");
    }

    public ClientConnection(HazelcastClientInstanceImpl client,
                            int connectionId) throws IOException {
        this.client = client;
        this.connectionManager = client.getConnectionManager();
        this.lifecycleService = client.getLifecycleService();
        this.connectionId = connectionId;
        writeHandler = null;
        readHandler = null;
        socketChannelWrapper = null;
        logger = client.getLoggingService().getLogger(ClientConnection.class);
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
        if (!live.get()) {
            if (logger.isFinestEnabled()) {
                logger.finest("Connection is closed, dropping frame -> " + frame);
            }
            return false;
        }
        writeHandler.enqueue(frame);
        return true;
    }

    public void init() throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put(stringToBytes(Protocols.CLIENT_BINARY_NEW));
        buffer.flip();
        socketChannelWrapper.write(buffer);
    }

    @Override
    public Address getEndPoint() {
        return remoteEndpoint;
    }

    @Override
    public boolean isAlive() {
        return live.get();
    }

    @Override
    public long lastReadTimeMillis() {
        return readHandler.getLastHandle();
    }

    @Override
    public long lastWriteTimeMillis() {
        return writeHandler.getLastHandle();
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
        return socketChannelWrapper.socket().getInetAddress();
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress) socketChannelWrapper.socket().getRemoteSocketAddress();
    }

    @Override
    public int getPort() {
        return socketChannelWrapper.socket().getPort();
    }

    public SocketChannelWrapper getSocketChannelWrapper() {
        return socketChannelWrapper;
    }

    public ClientConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public ClientReadHandler getReadHandler() {
        return readHandler;
    }

    public void setRemoteEndpoint(Address remoteEndpoint) {
        this.remoteEndpoint = remoteEndpoint;
    }

    public Address getRemoteEndpoint() {
        return remoteEndpoint;
    }

    public InetSocketAddress getLocalSocketAddress() {
        return (InetSocketAddress) socketChannelWrapper.socket().getLocalSocketAddress();
    }

    @Override
    public void close(String reason, Throwable cause) {
        if (!live.compareAndSet(true, false)) {
            return;
        }

        closeCause = cause;
        closeReason = reason;

        closedTime = System.currentTimeMillis();
        String message = this + " lost. Reason: ";
        if (cause != null) {
            message += cause.getClass().getName() + '[' + cause.getMessage() + ']';
        } else {
            message += "Socket explicitly closed";
        }

        try {
            innerClose();
        } catch (Exception e) {
            logger.warning(e);
        }

        if (lifecycleService.isRunning()) {
            logger.warning(message);
        } else {
            logger.finest(message);
        }
    }

    protected void innerClose() throws IOException {
        if (socketChannelWrapper.isOpen()) {
            socketChannelWrapper.close();
        }
        readHandler.shutdown();
        writeHandler.shutdown();

        MetricsRegistryImpl metricsRegistry = client.getMetricsRegistry();
        metricsRegistry.deregister(this);
        metricsRegistry.deregister(writeHandler);
        metricsRegistry.deregister(readHandler);
    }

    @Override
    public Throwable getCloseCause() {
        return closeCause;
    }

    @Override
    public String getCloseReason() {
        return closeReason;
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

    public long getLastHeartbeatRequestedMillis() {
        return lastHeartbeatRequestedMillis;
    }

    public long getLastHeartbeatReceivedMillis() {
        return lastHeartbeatReceivedMillis;
    }

    public boolean isHeartBeating() {
        return live.get() && isHeartBeating;
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
                + "live=" + live
                + ", connectionId=" + connectionId
                + ", socketChannel=" + socketChannelWrapper
                + ", remoteEndpoint=" + remoteEndpoint
                + ", lastReadTime=" + timeToStringFriendly(lastReadTimeMillis())
                + ", lastWriteTime=" + timeToStringFriendly(lastWriteTimeMillis())
                + ", closedTime=" + timeToStringFriendly(closedTime)
                + ", lastHeartbeatRequested=" + timeToStringFriendly(lastHeartbeatRequestedMillis)
                + ", lastHeartbeatReceived=" + timeToStringFriendly(lastHeartbeatReceivedMillis)
                + ", connected server version=" + connectedServerVersionString
                + '}';
    }

    public long getClosedTime() {
        return closedTime;
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

    public List<Member> getClientUnregisteredMembers() {
        return clientUnregisteredMembers;
    }

    public void setClientUnregisteredMembers(List<Member> clientUnregisteredMembers) {
        this.clientUnregisteredMembers = clientUnregisteredMembers;
    }
}
