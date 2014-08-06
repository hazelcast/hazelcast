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

package com.hazelcast.client.connection;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.nio.Address;

/**
 * Responsible for managing {@link com.hazelcast.client.connection.nio.ClientConnection} objects.
 */
public interface ClientConnectionManager {

    void shutdown();

    void start();

    /**
     * Tries to connect to an address in member list.
     * Gets an address a hint first tries that if not successful, tries connections from LoadBalancer
     *
     * @param address hintAddress
     * @return authenticated connection
     * @throws Exception authentication failed or no connection found
     */
    ClientConnection tryToConnect(Address address) throws Exception;

    /**
     * Creates a new owner connection to given address
     *
     * @param address to be connection to established
     * @return ownerConnection
     * @throws Exception
     */
    ClientConnection ownerConnection(Address address) throws Exception;

    /**
     * Called when an owner connection is closed
     */
    void onCloseOwnerConnection();

    /**
     * Removes event handler corresponding to callId from responsible ClientConnection
     *
     * @param callId of event handler registration request
     * @return true if found and removed, false otherwise
     */
    boolean removeEventHandler(Integer callId);
}
