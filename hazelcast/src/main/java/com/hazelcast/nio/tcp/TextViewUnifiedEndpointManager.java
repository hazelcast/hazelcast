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

package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.EndpointManager;
import com.hazelcast.nio.Packet;

import java.util.Collection;
import java.util.Set;

class TextViewUnifiedEndpointManager
        implements EndpointManager<TcpIpConnection> {

    private final TcpIpUnifiedEndpointManager unifiedEndpointManager;
    private final boolean rest;

    TextViewUnifiedEndpointManager(TcpIpUnifiedEndpointManager unifiedEndpointManager, boolean rest) {
        this.unifiedEndpointManager = unifiedEndpointManager;
        this.rest = rest;
    }

    @Override
    public Set<TcpIpConnection> getActiveConnections() {
        return rest ? unifiedEndpointManager.getRestConnections() : unifiedEndpointManager.getMemachedConnections();
    }

    @Override
    public Collection<TcpIpConnection> getConnections() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        unifiedEndpointManager.addConnectionListener(listener);
    }

    @Override
    public void accept(Packet packet) {
        unifiedEndpointManager.accept(packet);
    }

    @Override
    public boolean registerConnection(Address remoteEndPoint, TcpIpConnection connection) {
        return unifiedEndpointManager.registerConnection(remoteEndPoint, connection);
    }

    @Override
    public TcpIpConnection getConnection(Address address) {
        return unifiedEndpointManager.getConnection(address);
    }

    @Override
    public TcpIpConnection getOrConnect(Address address) {
        return unifiedEndpointManager.getOrConnect(address);
    }

    @Override
    public TcpIpConnection getOrConnect(Address address, boolean silent) {
        return unifiedEndpointManager.getOrConnect(address, silent);
    }

    @Override
    public boolean transmit(Packet packet, TcpIpConnection connection) {
        return unifiedEndpointManager.transmit(packet, connection);
    }

    @Override
    public boolean transmit(Packet packet, Address target) {
        return unifiedEndpointManager.transmit(packet, target);
    }

    @Override
    public String toString() {
        return unifiedEndpointManager.toString();
    }
}
