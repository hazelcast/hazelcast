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

/**
 * Responsible for managing {@link com.hazelcast.client.connection.nio.ClientConnection} instances.
 *
 * All methods are thread-safe
 */
public interface ClientConnectionManager extends ConnectionListenable {

    /**
     * Check if this ClientConnectionManager is alive.
     *
     * The ClientConnectionManager is not alive when client is closing.
     *
     * @return true if alive, false otherwise.
     */
    boolean isAlive();

    /**
     * Gets the Connection to the given member address.
     *
     * If address is null, null is returned.
     *
     * @param address to be connected
     * @return connection if available, null otherwise
     */
    Connection getActiveConnection(Address address);

    /**
     * Gets the Connection to the given member address.
     *
     * If the connection already is established, the connection is returned.
     *
     * If the connection is not yet established, it is created and the new connection
     * is returned. So this call is synchronous. For an asynchronous version
     * {@link #getOrTriggerConnect(Address, boolean)}
     *
     * @param address to be connected
     * @return associated connection if available, creates new connection otherwise
     * @throws IOException if connection is not established
     */
    Connection getOrConnect(Address address) throws IOException;

    /**
     * Gets the connection if it already is created or asynchronously creates the connection
     * and <code>null</code>null.
     *
     * @param address to be connected
     * @param acquiresResource
     * @return associated connection if available, returns null and triggers new connection creation otherwise
     * @throws IOException if connection is not able to triggered
     */
    Connection getOrTriggerConnect(Address address, boolean acquiresResource) throws IOException;

    /**
     * Gets all connections.
     *
     * @return all connections.
     */
    Collection<ClientConnection> getActiveConnections();

    /**
     * Gets the address of the member the client currently sees as owner.
     *
     * @return the address of the owning member.
     */
    Address getOwnerConnectionAddress();

    /**
     * Returns the ClientPrincipal.
     *
     * Could be null if the client has not authenticated.
     *
     * @return the ClientPrincipal
     */
    ClientPrincipal getPrincipal();

    /**
     * Gets the connection to the member this client currently sees as owner.
     *
     * Null is returned when the client doesn't know who the owner is, or if
     * the connection is not established.
     *
     * @return the ClientConnection to the owning member.
     */
    ClientConnection getOwnerConnection();
}
