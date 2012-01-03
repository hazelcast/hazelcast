/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.nio;

import com.hazelcast.logging.ILogger;
import com.hazelcast.util.SimpleBoundedQueue;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.logging.Level;

public final class Connection {
    final SocketChannelWrapper socketChannel;

    final ReadHandler readHandler;

    final WriteHandler writeHandler;

    final ConnectionManager connectionManager;

    final InOutSelector inOutSelector;

    private volatile boolean live = true;

    private volatile Type type = Type.NONE;

    Address endPoint = null;

    private final ILogger logger;

    private final int connectionId;

    private final SimpleBoundedQueue<Packet> packetQueue = new SimpleBoundedQueue<Packet>(100);

    private ConnectionMonitor monitor;

    public Connection(ConnectionManager connectionManager, InOutSelector inOutSelector, int connectionId, SocketChannelWrapper socketChannel) {
        this.inOutSelector = inOutSelector;
        this.connectionId = connectionId;
        this.logger = connectionManager.ioService.getLogger(Connection.class.getName());
        this.connectionManager = connectionManager;
        this.socketChannel = socketChannel;
        this.writeHandler = new WriteHandler(this);
        this.readHandler = new ReadHandler(this);
    }

    public Type getType() {
        return type;
    }

    public void releasePacket(Packet packet) {
        packetQueue.offer(packet);
    }

    public Packet obtainPacket() {
        Packet packet = packetQueue.poll();
        if (packet == null) {
            packet = new Packet();
        } else {
            packet.reset();
        }
        return packet;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public enum Type {
        NONE(false, false),
        MEMBER(true, true),
        JAVA_CLIENT(false, true),
        REST_CLIENT(false, false),
        MEMCACHE_CLIENT(false, false);

        final boolean member;
        final boolean binary;

        Type(boolean member, boolean binary) {
            this.member = member;
            this.binary = binary;
        }

        public boolean isBinary() {
            return binary;
        }

        public boolean isClient() {
            return !member;
        }
    }

    public boolean isClient() {
        return (type != null) && type != Type.NONE && type.isClient();
    }

    public void setType(Type type) {
        if (this.type == Type.NONE) {
            this.type = type;
        }
    }

    public SocketChannelWrapper getSocketChannelWrapper() {
        return socketChannel;
    }

    public ReadHandler getReadHandler() {
        return readHandler;
    }

    public WriteHandler getWriteHandler() {
        return writeHandler;
    }

    public InOutSelector getInOutSelector() {
        return inOutSelector;
    }

    public boolean live() {
        return live;
    }

    public Address getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(Address endPoint) {
        this.endPoint = endPoint;
    }

    public void setMonitor(ConnectionMonitor monitor) {
        this.monitor = monitor;
    }

    public ConnectionMonitor getMonitor() {
        return monitor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Connection)) return false;
        Connection that = (Connection) o;
        return connectionId == that.getConnectionId();
    }

    @Override
    public int hashCode() {
        return connectionId;
    }

    private void close0() throws IOException {
        if (!live)
            return;
        live = false;
        if (socketChannel != null && socketChannel.isOpen()) {
            socketChannel.close();
        }
        readHandler.shutdown();
        writeHandler.shutdown();
    }

    public void close() {
        close(null);
    }

    public void close(Throwable t) {
        try {
            close0();
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
        logger.log(Level.INFO, "Connection lost " + this.socketChannel.socket().getRemoteSocketAddress());
        connectionManager.destroyConnection(this);
        connectionManager.ioService.disconnectExistingCalls(endPoint);
        if (t != null && monitor != null) {
            monitor.onError(t);
        }
    }

    public int getConnectionId() {
        return connectionId;
    }

    @Override
    public String toString() {
        final Socket socket = this.socketChannel.socket();
        final SocketAddress remoteSocketAddress = socket != null ? socket.getRemoteSocketAddress() : null;
        return "Connection [" + remoteSocketAddress + " -> " + endPoint + "] live=" + live + ", client=" + isClient() + ", type=" + type;
    }
}
