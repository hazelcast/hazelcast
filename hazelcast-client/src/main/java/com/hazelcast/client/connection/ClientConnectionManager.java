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
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;

/**
 * Responsible for managing {@link com.hazelcast.client.connection.nio.ClientConnection} objects.
 */
public interface ClientConnectionManager {

    /**
     * Shutdown clientConnectionManager
     */
    void shutdown();

    /**
     * Check if client connection manager is live.
     * ClientConnectionManager is not live only when client is closing.
     *
     * @return true if live, false otherwise.
     */
    boolean isLive();

    /**
     * Start clientConnectionManager
     */
    void start();

    ClientConnection connectToAddress(Address target) throws Exception;

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
     * @return unique uuid of local client if available, null otherwise
     */
    String getUuid();

    /**
     * Called when an connection is closed.
     * Clears related resources of given clientConnection.
     *
     * @param clientConnection closed connection
     */
    void onConnectionClose(ClientConnection clientConnection);

    /**
     * Called when a member left the cluster
     * @param address address of the member
     */
    void removeEndpoint(Address address);

    /**
     * Removes event handler corresponding to callId from responsible ClientConnection
     *
     * @param callId of event handler registration request
     * @return true if found and removed, false otherwise
     */
    boolean removeEventHandler(Integer callId);

    /**
     * Handles incoming network package
     *
     * @param packet to be processed
     */
    void handlePacket(Packet packet);

    /**
     * Next unique call id for request
     *
     * @return new unique callId
     */
    int newCallId();

    /**
     * Sends request and waits for response
     *
     * @param request    to be send
     * @param connection to send the request over
     * @return response of request
     * @throws Exception if a network connection occurs or response is an exception
     */
    Object sendAndReceive(ClientRequest request, ClientConnection connection) throws Exception;

    /**
     * Called heartbeat timeout is detected on a connection.
     *
     * @param connection to be marked.
     */
    void onDetectingUnresponsiveConnection(ClientConnection connection);

}
