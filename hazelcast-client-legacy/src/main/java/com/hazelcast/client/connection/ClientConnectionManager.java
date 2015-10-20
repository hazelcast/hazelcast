/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.connection;

import com.hazelcast.client.spi.impl.ConnectionHeartbeatListener;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.Packet;

import java.io.IOException;

/**
 * Responsible for managing {@link com.hazelcast.client.connection.nio.ClientConnection} objects.
 */
public interface ClientConnectionManager {

    /**
     * Shutdown clientConnectionManager
     */
    void shutdown();

    /**
     * Check if client connection manager is alive.
     * ClientConnectionManager is not alive only when client is closing.
     *
     * @return true if alive, false otherwise.
     */
    boolean isAlive();

    /**
     * Start clientConnectionManager
     */
    void start();

    /**
     * @param address to be connected
     * @return connection if available, null otherwise
     */
    Connection getConnection(Address address);

    /**
     * @param address       to be connected
     * @param authenticator Authenticator implementation to send appropriate Authentication Request after connection
     * @return associated connection if available, creates new connection otherwise
     * @throws IOException if connection is not established
     */
    Connection getOrConnect(Address address, Authenticator authenticator) throws IOException;

    /**
     * Destroys the connection
     * Clears related resources of given connection.
     * ConnectionListener.connectionRemoved is called on registered listeners.
     * <p/>
     * If connection is already destroyed before calling this then does nothing.
     *
     * @param connection to be closed
     */
    void destroyConnection(Connection connection);

    /**
     * Handles incoming network package
     *
     * @param packet to be processed
     */
    void handlePacket(Packet packet);

    void addConnectionListener(ConnectionListener connectionListener);

    void addConnectionHeartbeatListener(ConnectionHeartbeatListener connectionHeartbeatListener);

}
