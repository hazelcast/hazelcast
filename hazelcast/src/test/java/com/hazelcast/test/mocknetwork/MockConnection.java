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
 *
 */

package com.hazelcast.test.mocknetwork;

import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ExceptionUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

public class MockConnection implements Connection {

    protected final Address localEndpoint;
    protected final NodeEngineImpl nodeEngine;

    private final Address remoteEndpoint;
    volatile MockConnection localConnection;

    private volatile AtomicBoolean alive = new AtomicBoolean(true);

    public MockConnection(Address localEndpoint, Address remoteEndpoint, NodeEngineImpl nodeEngine) {
        this.localEndpoint = localEndpoint;
        this.remoteEndpoint = remoteEndpoint;
        this.nodeEngine = nodeEngine;
    }

    @Override
    public Throwable getCloseCause() {
        return null;
    }

    @Override
    public String getCloseReason() {
        return null;
    }

    public Address getEndPoint() {
        return remoteEndpoint;
    }

    public boolean write(OutboundFrame frame) {
        if (!alive.get()) {
            return false;
        }

        if (nodeEngine.getNode().getState() == NodeState.SHUT_DOWN) {
            return false;
        }

        Packet packet = (Packet) frame;
        Packet newPacket = readFromPacket(packet);
        nodeEngine.getPacketDispatcher().dispatch(newPacket);
        return true;
    }

    private Packet readFromPacket(Packet packet) {
        Packet newPacket = new Packet();
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        boolean writeDone;
        boolean readDone;
        do {
            writeDone = packet.writeTo(buffer);
            buffer.flip();
            readDone = newPacket.readFrom(buffer);
            if (buffer.hasRemaining()) {
                throw new IllegalStateException("Buffer should be empty! " + buffer);
            }
            buffer.clear();
        } while (!writeDone);

        if (!readDone) {
            throw new IllegalStateException("Read should be completed!");
        }

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
            NodeEngineImpl localNodeEngine = localConnection.nodeEngine;
            Node localNode = localNodeEngine.getNode();
            MockConnectionManager connectionManager = (MockConnectionManager) localNode.connectionManager;
            connectionManager.onClose(this);
        } else {
            //this is a client-member connection. we need to notify NodeEngine about a client connection being closed.
            MockConnectionManager connectionManager = (MockConnectionManager) nodeEngine.getNode().connectionManager;
            connectionManager.onClose(this);
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
            throw ExceptionUtil.rethrow(e);
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
        return alive.get() && nodeEngine.getNode().getState() != NodeState.SHUT_DOWN;
    }

    @Override
    public String toString() {
        return "MockConnection{" + "localEndpoint=" + localEndpoint + ", remoteEndpoint=" + remoteEndpoint + '}';
    }
}
