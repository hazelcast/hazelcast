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
import com.hazelcast.spi.BackoffPolicy;
import com.hazelcast.spi.WriteResult;
import com.hazelcast.spi.impl.ExponentialBackoffPolicy;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Tcp/Ip implementation of the {@link com.hazelcast.nio.Connection}.
 * <p/>
 * A Connection has 2 sides:
 * <ol>
 * <li>the side where it receives data from the remote  machine</li>
 * <li>the side where it sends data to the remote machine</li>
 * </ol>
 * <p/>
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
    private final BackoffPolicy backoffPolicy = new ExponentialBackoffPolicy();
    private volatile int backoffState;
    private volatile long dontAskForSlotsBefore;
    private final boolean backPressureEnabled;

    public TcpIpConnection(TcpIpConnectionManager connectionManager, IOSelector in, IOSelector out,
                           int connectionId, SocketChannelWrapper socketChannel, boolean backPressureEnabled) {
        this.connectionId = connectionId;
        this.logger = connectionManager.ioService.getLogger(TcpIpConnection.class.getName());
        this.connectionManager = connectionManager;
        this.socketChannel = socketChannel;
        this.readHandler = new ReadHandler(this, in);
        this.writeHandler = new WriteHandler(this, out);
        this.backPressureEnabled = backPressureEnabled;
    }

    @Override
    public ConnectionType getType() {
        return type;
    }

    public TcpIpConnectionManager getConnectionManager() {
        return connectionManager;
    }

    @Override
    public boolean isFull() {
        //logger.severe("availableSlots:"+availableSlots.get());
        return availableSlots.get() <= 0;
    }

    @Override
    public WriteResult writeBackup(Packet packet) {
        if (!live) {
            if (logger.isFinestEnabled()) {
                logger.finest("Connection is closed, won't write packet -> " + packet);
            }
            return WriteResult.FAILURE;
        }

        if (!backPressureEnabled) {
            writeHandler.enque(packet);
            return WriteResult.SUCCESS;
        }

        boolean full;
        for (; ; ) {
            int oldAvailableSlots = availableSlots.get();

            full = oldAvailableSlots <= 0;

            int newAvailableSlots = oldAvailableSlots - 1;
            if (availableSlots.compareAndSet(oldAvailableSlots, newAvailableSlots)) {
                break;
            }
        }

        if (full) {
            // if the connection is full, we are going to try to send a claim.
            if (waitingForSlotResponse.compareAndSet(false, true)) {
                long askInMs = dontAskForSlotsBefore - Clock.currentTimeMillis();
                if (askInMs <= 0) {
                    sendClaim();
                } else {
                    waitingForSlotResponse.set(false);
                }
            }
        }

        // we are going to the packet no matter the queue is full because we can't store it locally.
        // because the future will not be waiting for all backups to complete, it will provide the back pressure
        writeHandler.enque(packet);
        return full ? WriteResult.FULL : WriteResult.SUCCESS;
    }

    @Override
    public WriteResult write(SocketWritable packet) {
        if (!live) {
            if (logger.isFinestEnabled()) {
                logger.finest("Connection is closed, won't write packet -> " + packet);
            }
            return WriteResult.FAILURE;
        }

        if (backPressureEnabled) {
            if (packet.isBackpressureAllowed()) {
                return enqueWithBackPressure(packet);
            }

        }
        writeHandler.enque(packet);
        return WriteResult.SUCCESS;
    }

    private WriteResult enqueWithBackPressure(SocketWritable packet) {
        WriteResult result = writeIfSlotAvailable(packet);
        if (result != WriteResult.FULL) {
            return result;
        }

        if (waitingForSlotResponse.compareAndSet(false, true)) {
            long askInMs = dontAskForSlotsBefore - Clock.currentTimeMillis();
            if (askInMs <= 0) {
                sendClaim();
            } else {
//                if (logger.isFinestEnabled()) {
//                logger.info("No slots to " + toString() + " are available, but I can only ask for new ones in " + askInMs + " ms.");
//                }
                waitingForSlotResponse.set(false);
            }
            return WriteResult.FULL;
        } else {
            return writeIfSlotAvailable(packet);
        }
    }

    private WriteResult writeIfSlotAvailable(SocketWritable packet) {
        int availSlotsNow = availableSlots.decrementAndGet();
        if (availSlotsNow >= 0) {
            writeHandler.enque(packet);
            return WriteResult.SUCCESS;
        }
        return WriteResult.FULL;
    }

    private void sendClaim() {
        //todo: needs to be removed or put under debug
        //      logger.info("Sending claim for " + toString());
        IOService ioService = connectionManager.ioService;
        Data dummyData = ioService.toData(0);
        Packet slotRequestPacket = new Packet(dummyData, ioService.getPortableContext());
        slotRequestPacket.setHeader(Packet.HEADER_CLAIM);
        slotRequestPacket.setHeader(Packet.HEADER_URGENT);
        writeHandler.enque(slotRequestPacket);
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
    public void setAvailableSlots(int claimResponse) {
        if (claimResponse == 0) {
            backoffState = backoffPolicy.nextState(backoffState);
            dontAskForSlotsBefore = Clock.currentTimeMillis() + backoffState;
            //    logger.info("Received empty claim response from " + toString() + ". Next attempt in " + backoffState + " ms.");
        } else {
            //    logger.info("Received " + claimResponse + "slots for " + toString());
            backoffState = BackoffPolicy.EMPTY_STATE;
        }

        for (; ; ) {
            int currentAvailableSlots = this.availableSlots.get();
            if (currentAvailableSlots > 0) {
                //we are going to ignore any claimResponses if the availableSlots is bigger than zero
                break;
            } else {
                int newAvailableSlots = currentAvailableSlots + claimResponse;
                if (availableSlots.compareAndSet(currentAvailableSlots, newAvailableSlots)) {
                    waitingForSlotResponse.set(false);
                    break;
                }
            }
        }
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
