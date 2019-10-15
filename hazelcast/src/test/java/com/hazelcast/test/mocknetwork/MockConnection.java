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

package com.hazelcast.test.mocknetwork;

import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionLifecycleListener;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.test.mocknetwork.MockNetworkingService.MockEndpointManager.isTargetLeft;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static org.junit.Assert.assertNotNull;

public class MockConnection implements Connection {

    protected final Address localEndpoint;
    protected final NodeEngineImpl remoteNodeEngine;

    volatile MockConnection localConnection;

    private volatile ConnectionLifecycleListener lifecycleListener;

    private final AtomicBoolean alive = new AtomicBoolean(true);

    private final Address remoteEndpoint;

    private final EndpointManager endpointManager;

    public MockConnection(Address localEndpoint,
                          Address remoteEndpoint, NodeEngineImpl remoteNodeEngine) {
        this(null, localEndpoint, remoteEndpoint, remoteNodeEngine, null);
    }

    public MockConnection(ConnectionLifecycleListener lifecycleListener, Address localEndpoint,
                          Address remoteEndpoint, NodeEngineImpl remoteNodeEngine, EndpointManager localEndpointManager) {
        this.lifecycleListener = lifecycleListener;
        this.localEndpoint = localEndpoint;
        this.remoteEndpoint = remoteEndpoint;
        this.remoteNodeEngine = remoteNodeEngine;
        this.endpointManager = localEndpointManager;
    }

    @Override
    public EndpointManager getEndpointManager() {
        return endpointManager;
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
    public Address getEndPoint() {
        return remoteEndpoint;
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
            buffer.flip();
            newPacket = packetReader.readFrom(buffer);
            if (buffer.hasRemaining()) {
                throw new IllegalStateException("Buffer should be empty! " + buffer);
            }
            buffer.clear();
        } while (!writeDone);

        assertNotNull(newPacket);
        newPacket.setConn(localConnection);
        return newPacket;
    }

    public long lastReadTimeMillis() {
        return System.currentTimeMillis();
    }

    public long lastWriteTimeMillis() {
        return System.currentTimeMillis();
    }

    public void close(String msg, Throwable cause) {
        if (!alive.compareAndSet(true, false)) {
            return;
        }

        if (localConnection != null) {
            //this is a member-to-member connection
            localConnection.close(msg, cause);
        }

        if (lifecycleListener != null) {
            lifecycleListener.onConnectionClose(this, cause, false);
        }
    }

    @Override
    public void setType(ConnectionType type) {
        //NO OP
    }

    public boolean isClient() {
        return false;
    }

    @Override
    public ConnectionType getType() {
        return ConnectionType.MEMBER;
    }

    public InetAddress getInetAddress() {
        try {
            return localEndpoint.getInetAddress();
        } catch (UnknownHostException e) {
            throw rethrow(e);
        }
    }

    public InetSocketAddress getRemoteSocketAddress() {
        return new InetSocketAddress(getInetAddress(), getPort());
    }

    public int getPort() {
        return localEndpoint.getPort();
    }

    @Override
    public boolean isAlive() {
        return alive.get() && !isTargetLeft(remoteNodeEngine.getNode());
    }

    @Override
    public String toString() {
        return "MockConnection{"
                + "localEndpoint=" + localEndpoint
                + ", remoteEndpoint=" + remoteEndpoint
                + ", alive=" + isAlive() + '}';
    }

    @Override
    public Certificate[] getRemoteCertificates() {
        return null;
    }
}
