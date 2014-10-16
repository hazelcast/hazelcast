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

package com.hazelcast.nio.tcp;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.SocketWritable;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.WriteResult;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Tcp/Ip implementation of the {@link com.hazelcast.nio.Connection}.
 *
 * A Connection has 2 sides:
 * <ol>
 *     <li>the side where it receives data from the remote  machine</li>
 *     <li>the side where it sends data to the remote machine</li>
 * </ol>
 *
 * The reading side is the {@link com.hazelcast.nio.tcp.ReadHandler} and the writing side of this connection
 * is the {@link com.hazelcast.nio.tcp.WriteHandler}.
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

    private final int connectionId;

    private TcpIpConnectionMonitor monitor;

    private final AtomicInteger availableSlots = new AtomicInteger();
    private final AtomicBoolean waitingForSlotResponse = new AtomicBoolean();

    public TcpIpConnection(TcpIpConnectionManager connectionManager, IOSelector in, IOSelector out,
                           int connectionId, SocketChannelWrapper socketChannel) {
        this.connectionId = connectionId;
        this.logger = connectionManager.ioService.getLogger(TcpIpConnection.class.getName());
        this.connectionManager = connectionManager;
        this.socketChannel = socketChannel;
        this.readHandler = new ReadHandler(this, in);
        this.writeHandler = new WriteHandler(this, out);
    }

    @Override
    public ConnectionType getType() {
        return type;
    }

    public TcpIpConnectionManager getConnectionManager() {
        return connectionManager;
    }

    @Override
    public WriteResult write(SocketWritable packet) {
        if (!live) {
            if (logger.isFinestEnabled()) {
                logger.finest("Connection is closed, won't write packet -> " + packet);
            }
            return WriteResult.FAILURE;
        }
        if (packet.isBackpressureAllowed()) {
            return writeWithBackpressureApplied(packet);
        } else {
            writeHandler.enqueueSocketWritable(packet);
            return WriteResult.SUCCESS;
        }
    }

    private WriteResult writeWithBackpressureApplied(SocketWritable packet) {
        WriteResult result = writeIfSlotAvailable(packet);
        if (result != WriteResult.FULL) {
            return result;
        }
        if (waitingForSlotResponse.compareAndSet(false, true)) {
            requestNewSlots();
            return WriteResult.FULL;
        } else {
            return writeIfSlotAvailable(packet);
        }
    }

    private WriteResult writeIfSlotAvailable(SocketWritable packet) {
        int availSlotsNow = availableSlots.decrementAndGet();
        if (availSlotsNow >= 0) {
            writeHandler.enqueueSocketWritable(packet);
            return WriteResult.SUCCESS;
        }
        return WriteResult.FULL;
    }

    private void requestNewSlots() {
        IOService ioService = connectionManager.ioService;
        Data dummyData = ioService.toData(0);
        Packet slotRequestPacket = new Packet(dummyData, ioService.getPortableContext());
        slotRequestPacket.setHeader(Packet.HEADER_CLAIM_REQ);
        slotRequestPacket.setHeader(Packet.HEADER_URGENT);
        writeHandler.enqueueSocketWritable(slotRequestPacket);
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
    public void setAvailableSlots(Integer claimResponse) {
        availableSlots.set(claimResponse);
        waitingForSlotResponse.set(false);
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
    public boolean isAlive() {
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

    public void setMonitor(TcpIpConnectionMonitor monitor) {
        this.monitor = monitor;
    }

    public TcpIpConnectionMonitor getMonitor() {
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
            readHandler.shutdown();
            writeHandler.shutdown();
            socketChannel.close();
        }
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
        Socket socket = this.socketChannel.socket();
        SocketAddress localSocketAddress = socket != null ? socket.getLocalSocketAddress() : null;
        SocketAddress remoteSocketAddress = socket != null ? socket.getRemoteSocketAddress() : null;
        return "Connection [" + localSocketAddress + " -> " + remoteSocketAddress
                + "], endpoint=" + endPoint + ", live=" + live + ", type=" + type;
    }
}
