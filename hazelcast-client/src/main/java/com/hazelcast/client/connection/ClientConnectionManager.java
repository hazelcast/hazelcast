/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListenable;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Future;

/**
 * Responsible for managing {@link com.hazelcast.client.connection.nio.ClientConnection} objects.
 */
public interface ClientConnectionManager extends ConnectionListenable {

    /**
     * Check if client connection manager is alive.
     * ClientConnectionManager is not alive only when client is closing.
     *
     * @return true if alive, false otherwise.
     */
    boolean isAlive();

    /**
     * Gets the connection for the given member address.
     *
     * If the connection is not established, null is returned. There will not be
     * an attempt to create the connection.
     *
     * If address is null, null is returned.
     *
     * @param address of the member
     * @return connection if available, null otherwise
     */
    Connection getConnection(Address address);

    /**
     * Gets the connection if it exist, or otherwise try to connect.
     *
     * If the connection to the given member already is established, return that connection.
     *
     * if the connection doesn't exist, the connection is established and the connection returned.
     *
     * @param address the address of the member.
     * @return associated connection if available, creates new connection otherwise
     * @throws IOException if connection failed to be not established
     */
    Connection getOrConnect(Address address) throws IOException;

    /**
     * Gets the connection if it exists, or asynchronously tries to establish the connection.
     *
     * If the connection is not yet established, null is returned and the ClientConnectionManager
     * will asynchronously try once to make the connection.
     *
     * @param address the address of the member
     * @return associated connection if available
     * @throws IOException if connection is not able to triggered
     */
    Connection getOrAsyncConnect(Address address, boolean acquiresResource) throws IOException;

    Collection<ClientConnection> getActiveConnections();

    Address getOwnerConnectionAddress();

    ClientPrincipal getPrincipal();

    ClientConnection getOwnerConnection();

    void connectToCluster();

    Future<Void> connectToClusterAsync();
}
