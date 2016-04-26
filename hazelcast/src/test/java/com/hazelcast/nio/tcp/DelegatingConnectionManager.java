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
 *
 */

package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.util.Preconditions;

public abstract class DelegatingConnectionManager implements ConnectionManager, PacketHandler {

    protected final ConnectionManager connectionManager;
    protected final PacketHandler packetHandler;

    protected DelegatingConnectionManager(ConnectionManager connectionManager) {
        Preconditions.checkNotNull(connectionManager);

        this.connectionManager = connectionManager;

        if (connectionManager instanceof PacketHandler) {
            packetHandler = (PacketHandler) connectionManager;
        } else {
            packetHandler = new PacketHandler() {
                @Override
                public void handle(Packet packet) throws Exception {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    @Override
    public boolean transmit(Packet packet, Connection connection) {
        return connectionManager.transmit(packet, connection);
    }

    @Override
    public boolean transmit(Packet packet, Address target) {
        return connectionManager.transmit(packet, target);
    }

    @Override
    public void handle(Packet packet) throws Exception {
        packetHandler.handle(packet);
    }

    @Override
    public int getCurrentClientConnections() {return connectionManager.getCurrentClientConnections();}

    @Override
    public int getAllTextConnections() {return connectionManager.getAllTextConnections();}

    @Override
    public int getConnectionCount() {return connectionManager.getConnectionCount();}

    @Override
    public int getActiveConnectionCount() {return connectionManager.getActiveConnectionCount();}

    @Override
    public Connection getConnection(Address address) {return connectionManager.getConnection(address);}

    @Override
    public Connection getOrConnect(Address address) {return connectionManager.getOrConnect(address);}

    @Override
    public Connection getOrConnect(Address address, boolean silent) {
        return connectionManager.getOrConnect(address, silent);
    }

    @Override
    public boolean registerConnection(Address address, Connection connection) {
        return connectionManager.registerConnection(address, connection);
    }

    @Override
    public void destroyConnection(Connection connection) {connectionManager.destroyConnection(connection);}

    @Override
    public void addConnectionListener(ConnectionListener listener) {connectionManager.addConnectionListener(listener);}

    @Override
    public void start() {connectionManager.start();}

    @Override
    public void stop() {connectionManager.stop();}

    @Override
    public void shutdown() {connectionManager.shutdown();}
}
