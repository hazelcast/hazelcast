/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.server;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListenable;
import com.hazelcast.internal.nio.Packet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Responsible for managing {@link ServerConnection} instances.
 */
public interface ServerConnectionManager
        extends ConnectionListenable<ServerConnection>, Consumer<Packet> {

    /**
     * Returns all connections managed by this ServerConnectionManager.
     *
     * In case of a member connection, it will also return connections that have not yet completed the
     * {@link com.hazelcast.internal.cluster.impl.MemberHandshake}.
     */
    @Nonnull
    Collection<ServerConnection> getConnections();

    /**
     * Returns the number of connections that satisfy some predicate.
     *
     * @param predicate the Predicate. Predicate can be null which means that no filtering is done.
     * @return the count
     */
    default int connectionCount(@Nullable Predicate<ServerConnection> predicate) {
        if (predicate == null) {
            return getConnections().size();
        }

        return (int) getConnections().stream().filter(predicate).count();
    }

    /**
     * Registers (i.e. stores) the connection for the given primaryAddress.
     * Once this call finishes every subsequent call to {@link #get(Address)}
     * will return the relevant {@link Connection} resource.
     *
     * @param primaryAddress - The primary address to register the connection under
     * @param connection    - The connection to be registered
     * @return True if the call was successful
     */
    default boolean register(Address primaryAddress, UUID remoteUuid, ServerConnection connection) {
        return register(primaryAddress, primaryAddress, null, remoteUuid, connection, 0);
    }

    /**
     * Registers (i.e. stores) the connection for the given primary address.
     * Once this call finishes every subsequent call to {@link #get(Address)} will return
     * the relevant {@link Connection} resource.
     *
     * @param primaryAddress - The primary address to register the connection under.
     *                       If the remote side of connection is a Hazelcast member, we prefer one
     *                       of the available public addresses as a primary address. If the underlying
     *                       connection manager supports multiple endpoints, the public address corresponding
     *                       to {@link com.hazelcast.instance.EndpointQualifier#MEMBER} has the priority.
     * @param targetAddress - Requested target address on the connector side, null if acceptor side.
     *                      We register this targetAddress as an alias to primaryAddress and also remove
     *                      the connection from in progress connections set by using this target address.
     *      address.
     * @param remoteAddressAliases the other address aliases to be registered which are incoming as part of remote
     *                            member's MemberHandshake
     * @param connection    - The connection to be registered
     * @param planeIndex    - The index of the plane
     * @return True if the call was successful
     */
    boolean register(
            Address primaryAddress,
            Address targetAddress,
            Collection<Address> remoteAddressAliases,
            UUID remoteUuid,
            ServerConnection connection,
            int planeIndex
    );

    /**
     * Returns the number of connections.
     *
     * @return the number of connections.
     */
    default int connectionCount() {
        return connectionCount(null);
    }

    /**
     * Gets the connection for a given address. If the connection does not exist, it returns null.
     *
     * @param address the address of remote side of the connection
     * @return the found Connection, or none if one doesn't exist
     */
    @Nullable
    default ServerConnection get(@Nonnull Address address) {
        return get(address, 0);
    }

    /**
     * Gets the connection for a given address and streamId. If the connection
     * does not exist, it returns null.
     *
     * @param address the address of remote side of the connection
     * @param streamId the stream id
     * @return the found Connection, or none if one doesn't exist
     */
    @Nullable
    ServerConnection get(@Nonnull Address address, int streamId);

    /**
     * Returns all the registered connections that belong to the given address.
     * Note that this method doesn't return the member connections whose
     * {@link com.hazelcast.internal.cluster.impl.MemberHandshake} aren't
     * processed yet and the client connections whose authentications aren't
     * yet completed. If there is no registered connection exists for the given
     * address, it returns an empty list.
     *
     * @param address the address of connections
     * @return the list of connections that belong to the given address or an
     * empty list if one doesn't exist
     */
    @Nonnull
    List<ServerConnection> getAllConnections(@Nonnull Address address);

    /**
     * Gets the existing connection for a given address or connects.
     * <p>
     * Default implementation is equivalent to calling:
     * <pre>{@code
     * getOrConnect(address, false)
     * }</pre>
     *
     * @param address the address to connect to
     * @return the found connection, or {@code null} if no connection exists
     * @see #getOrConnect(Address, boolean)
     */
    @Nullable
    default ServerConnection getOrConnect(@Nonnull Address address) {
        return getOrConnect(address, false, 0);
    }

    /**
     * Gets the existing connection for a given address or connects.
     * <p>
     * Default implementation is equivalent to calling:
     * <pre>{@code
     * getOrConnect(address, false)
     * }</pre>
     *
     * @param address the address to connect to
     * @param streamId the stream id
     * @return the found connection, or {@code null} if no connection exists
     * @see #getOrConnect(Address, boolean)
     */
    @Nullable
    ServerConnection getOrConnect(@Nonnull Address address, int streamId);

    /**
     * Gets the existing connection for a given address. If it does not exist, the system will try to connect
     * asynchronously. In this case, it returns {@code null}.
     * <p>
     * When the connection is established at some point in time, it can be retrieved using the
     * {@link #get(Address)}.
     *
     * @param address the address to connect to
     * @param silent   connection errors are reported to error handler and logged on info level when {@code false},
     *                 otherwise errors are not reported and logged on debug level
     * @return the existing connection
     */
    @Nullable
    default ServerConnection getOrConnect(@Nonnull Address address, boolean silent) {
        return getOrConnect(address, silent, 0);
    }

    /**
     * Gets the existing connection for a given address. If it does not exist, the system will try to connect
     * asynchronously. In this case, it returns {@code null}.
     * <p>
     * When the connection is established at some point in time, it can be retrieved using the
     * {@link #get(Address)}.
     *
     * @param address the address to connect to
     * @param silent   connection errors are reported to error handler and logged on info level when {@code false},
     *                 otherwise errors are not reported and logged on debug level
     * @param streamId the stream id
     * @return the existing connection
     */
    @Nullable
    ServerConnection getOrConnect(@Nonnull Address address, boolean silent, int streamId);

    /**
     * Transmits a packet to a certain address.
     * <p>
     * If the connection to the target doesn't exist yet, the system will try to make the connection. In this case
     * true can be returned, even though the connection eventually can't be established.
     *
     * @param packet The Packet to transmit.
     * @param targetAddress The address of the target machine where the Packet should be transmitted.
     * @return true if the transmit was a success, false if a failure.
     * @throws NullPointerException if packet or target is null.
     */
    default boolean transmit(Packet packet, Address targetAddress) {
        return transmit(packet, targetAddress, 0);
    }

    /**
     * Transmits a packet to a certain address.
     * <p>
     * If the connection to the target doesn't exist yet, the system will try to make the connection. In this case
     * true can be returned, even though the connection eventually can't be established.
     *
     * @param packet The Packet to transmit.
     * @param targetAddress The address of the target machine where the packet should be transmitted.
     * @param streamId the stream id
     * @return true if the transmit was a success, false if a failure.
     * @throws NullPointerException if packet or target is null.
     */
    boolean transmit(Packet packet, Address targetAddress, int streamId);

    /**
     * Returns network stats for inbound and outbound traffic.
     * Stats are available only when Advanced Networking is enabled.
     *
     * @return network stats, or {@code null} if Advanced Networking is disabled
     */
    NetworkStats getNetworkStats();

    /**
     * Gets the Server this {@link ServerConnectionManager} belongs to.
     *
     * @return the Server.
     */
    Server getServer();

    /**
     * blocks the caller thread until a connection is established (or failed)
     * or the time runs out. Callers must ensure a connection is established after this method returns {@code true}.
     * @param address the address of the remote side of connection that we're waiting for its establishment
     * @param timeoutMillis the maximum time to block on
     * @param streamId the stream id for the connection
     * @return true if connected successfully, false if timed out
     * @throws java.lang.InterruptedException if the current thread was interrupted while blocking
     */
    default boolean blockOnConnect(Address address, long timeoutMillis, int streamId) throws InterruptedException {
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(timeoutMillis));
        return false;
    }
}
