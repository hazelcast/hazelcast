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

package com.hazelcast.test.mocknetwork;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.nio.ConnectionLifecycleListener;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static com.hazelcast.test.mocknetwork.MockServer.isTargetLeft;
import static org.junit.Assert.assertNotNull;

public class MockServerConnection implements ServerConnection {

    protected final Address localAddress;
    protected final NodeEngineImpl localNodeEngine;
    protected final NodeEngineImpl remoteNodeEngine;

    volatile MockServerConnection otherConnection;

    private volatile ConnectionLifecycleListener lifecycleListener;

    private final AtomicBoolean alive = new AtomicBoolean(true);

    private final Address remoteAddress;

    private final UUID localUuid;
    private volatile UUID remoteUuid;

    private final ServerConnectionManager connectionManager;

    private final ConcurrentMap attributeMap = new ConcurrentHashMap();

    public MockServerConnection(
            Address localAddress,
            Address remoteAddress,
            UUID localUuid,
            UUID remoteUuid,
            NodeEngineImpl localNodeEngine,
            NodeEngineImpl remoteNodeEngine
    ) {
        this(null, localAddress, remoteAddress, localUuid, remoteUuid, localNodeEngine, remoteNodeEngine, null);
    }

    public MockServerConnection(
            ConnectionLifecycleListener lifecycleListener,
            Address localAddress,
            Address remoteAddress,
            UUID localUuid,
            UUID remoteUuid,
            NodeEngineImpl localNodeEngine,
            NodeEngineImpl remoteNodeEngine,
            ServerConnectionManager localConnectionManager
    ) {
        this.lifecycleListener = lifecycleListener;
        this.localAddress = localAddress;
        this.remoteAddress = remoteAddress;
        this.localUuid = localUuid;
        this.remoteUuid = remoteUuid;
        this.localNodeEngine = localNodeEngine;
        this.remoteNodeEngine = remoteNodeEngine;
        this.connectionManager = localConnectionManager;
    }

    @Override
    public ConcurrentMap attributeMap() {
        return attributeMap;
    }

    @Override
    public ServerConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setLifecycleListener(ConnectionLifecycleListener lifecycleListener) {
        this.lifecycleListener = lifecycleListener;
    }

    @Override
    public Throwable getCloseCause() {
        return null;
    }

    @Override
    public String getCloseReason() {
        return null;
    }

    @Override
    public Address getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public UUID getRemoteUuid() {
        return remoteUuid;
    }

    public InetAddress getInetAddress() {
        try {
            return localAddress.getInetAddress();
        } catch (UnknownHostException e) {
            throw rethrow(e);
        }
    }

    public boolean write(OutboundFrame frame) {
        if (!isAlive()) {
            return false;
        }

        Packet packet = (Packet) frame;
        Packet newPacket = readFromPacket(packet);
        try {
            remoteNodeEngine.getPacketDispatcher().accept(newPacket);
        } catch (Exception e) {
            throw rethrow(e);
        }
        return true;
    }

    private Packet readFromPacket(Packet packet) {
        Packet newPacket;
        PacketIOHelper packetReader = new PacketIOHelper();
        PacketIOHelper packetWriter = new PacketIOHelper();
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        boolean writeDone;
        do {
            writeDone = packetWriter.writeTo(packet, buffer);
            upcast(buffer).flip();
            newPacket = packetReader.readFrom(buffer);
            if (buffer.hasRemaining()) {
                throw new IllegalStateException("Buffer should be empty! " + buffer);
            }
            upcast(buffer).clear();
        } while (!writeDone);

        assertNotNull(newPacket);
        newPacket.setConn(otherConnection);
        return newPacket;
    }

    public long lastReadTimeMillis() {
        return System.currentTimeMillis();
    }

    public long lastWriteTimeMillis() {
        return System.currentTimeMillis();
    }

    public void close(String msg, Throwable cause) {
        try {
            if (!alive.compareAndSet(true, false)) {
                return;
            }

            if (otherConnection != null) {
                otherConnection.close(msg, cause);
            }
        } finally {
            if (lifecycleListener != null) {
                lifecycleListener.onConnectionClose(this, cause, false);
            }
        }
    }

    @Override
    public void setConnectionType(String connectionType) {
        //NO OP
    }

    public boolean isClient() {
        return false;
    }

    @Override
    public String getConnectionType() {
        return ConnectionType.MEMBER;
    }

    public InetSocketAddress getRemoteSocketAddress() {
        InetAddress inetAddress;
        try {
            inetAddress = localAddress.getInetAddress();
        } catch (UnknownHostException e) {
            throw rethrow(e);
        }

        return new InetSocketAddress(inetAddress, localAddress.getPort());
    }

    @Override
    public void setRemoteAddress(Address remoteAddress) {
    }

    @Override
    public void setRemoteUuid(UUID remoteUuid) {
        this.remoteUuid = remoteUuid;
    }

    @Override
    public boolean isAlive() {
        return alive.get() && !isTargetLeft(remoteNodeEngine.getNode());
    }

    @Override
    public String toString() {
        return "MockConnection{"
                + "localEndpoint=[address=" + localAddress + ", uuid=" + localUuid + "]"
                + ", remoteEndpoint=[address=" + remoteAddress + ", uuid=" + remoteUuid + "]"
                + ", alive=" + isAlive() + '}';
    }
}
