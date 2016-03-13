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
 */

package com.hazelcast.nio;

/**
 * Responsible for managing {@link com.hazelcast.nio.Connection} objects.
 */
public interface ConnectionManager {


    /**
     * Gets the number of client connections.
     *
     * @return number of client connections.
     */
    int getCurrentClientConnections();

    /**
     * Gets the number of text connections (rest/memcache)
     *
     * @return the number of text connections.
     */
    int getAllTextConnections();

    /**
     * Gets the total number of connections.
     *
     * @return the total number of connections.
     */
    int getConnectionCount();

    /**
     * Gets the number of active connections.
     *
     * @return the number of active connections.
     */
    int getActiveConnectionCount();

    /**
     * Gets the connection for a given address. If the connection does not exist, it returns null.
     *
     * @param address the remote side of the connection.
     * @return the found Connection, or none if one doesn't exist.
     */
    Connection getConnection(Address address);

    /**
     * Gets the existing connection for a given address or connects. This call is silent.
     *
     * @param address the address to connect to.
     * @return the found connection, or null if no connection exists.
     * @see #getOrConnect(Address, boolean)
     */
    Connection getOrConnect(Address address);

    /**
     * Gets the existing connection for a given address. If it does not exist, the system will try to connect
     * asynchronously. In this case, it returns null.
     *
     * When the connection is established at some point in time, it can be retrieved using the
     * {@link #getConnection(Address)}.
     *
     * @param address the address to connect to.
     * @param silent if logging should be done on debug level (silent=true) or on info level (silent=false).
     * @return the existing connection.
     */
    Connection getOrConnect(Address address, boolean silent);

    boolean registerConnection(Address address, Connection connection);

    /**
     * Destroys a connection.
     *
     * If connection is null, the call is ignored.
     *
     * If the connection is already destroyed, the call is ignored.
     *
     * @param connection the Connection to destroy.
     */
    void destroyConnection(Connection connection);

    /**
     * Registers a ConnectionListener.
     *
     * If the same listener is registered multiple times, it will be notified multiple times.
     *
     * @param listener the ConnectionListener to add.
     * @throws NullPointerException if listener is null.
     */
    void addConnectionListener(ConnectionListener listener);

    /**
     * Transmits a packet to a certain connection.
     *
     * If this method is called with a null connection, the call returns false
     *
     * @param packet The Packet to transmit.
     * @param connection The connection to where the Packet should be transmitted.
     * @return true if the transmit was a success, false if a failure. There is no guarantee that the packet is actually going
     * to be received since the Packet perhaps is stuck in some buffer. It just means that it is buffered somewhere.
     * @throws NullPointerException if packet is null.
     */
    boolean transmit(Packet packet, Connection connection);

    /**
     * Transmits a packet to a certain address.
     *
     * If the connection to the target doesn't exist yet, the system will try to make the connection. In this case
     * true can be returned, even though the connection eventually can't be established.
     *
     * @param packet The Packet to transmit.
     * @param target The address of the target machine where the Packet should be transmitted.
     * @return true if the transmit was a success, false if a failure.
     * @see #transmit(com.hazelcast.nio.Packet, com.hazelcast.nio.Connection)
     * @throws NullPointerException if packet or target is null.
     */
    boolean transmit(Packet packet, Address target);

    /**
     * Starts ConnectionManager, initializes its resources, starts threads, etc. After start, ConnectionManager
     * becomes fully operational.
     * <p/>
     * If it is already started, then this method has no effect.
     *
     * @throws IllegalStateException if ConnectionManager is shutdown
     */
    void start();

    /**
     * Stops ConnectionManager, releases its resources, stops threads, etc. When stopped, ConnectionManager
     * can be started again using {@link #start()}.
     * <p/>
     * This method has no effect if it is already stopped or shutdown.
     * <p/>
     * Currently <tt>stop</tt> is called during the merge process to detach node from the current cluster. After
     * node becomes ready to join to the new cluster, <tt>start</tt> is called to re-initialize the ConnectionManager.
     */
    void stop();

    /**
     * Shutdowns ConnectionManager completely. ConnectionManager will not be operational anymore and cannot
     * be restarted.
     * <p/>
     * This method has no effect if it is already shutdown.
     */
    void shutdown();
}
