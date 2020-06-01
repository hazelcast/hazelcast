/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
    @Nonnull Collection<ServerConnection> getConnections();

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
     * Returns the number of connections.
     *
     * @return the number of connections.
     */
    default int connectionCount() {
        return connectionCount(null);
    }

    /**
     * Registers (i.e. stores) the connection for the given remote remoteAddress.
     * Once this call finishes every subsequent call to {@link #get(Address)} will return
     * the relevant {@link Connection} resource.
     *
     * @param remoteAddress - The remote address to register the connection under
     * @param connection    - The connection to be registered
     * @return True if the call was successful
     */
    boolean register(Address remoteAddress, ServerConnection connection);

    /**
     * Gets the connection for a given address. If the connection does not exist, it returns null.
     *
     * @param address the remote side of the connection
     * @return the found Connection, or none if one doesn't exist
     */
    ServerConnection get(Address address);

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
    default ServerConnection getOrConnect(Address address) {
        return getOrConnect(address, false);
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
     * @return the existing connection
     */
    ServerConnection getOrConnect(Address address, boolean silent);

    /**
     * Transmits a packet to a certain connection.
     * <p>
     * If this method is called with a {@code null} connection, the call returns {@code false}.
     *
     * @param packet     the packet to transmit
     * @param connection he connection to where the Packet should be transmitted
     * @return {@code true} if the transmit was a success, {@code false} if a failure (there is no guarantee that the packet is
     * actually going to be received since the packet perhaps is stuck in some buffer; it just means that it's buffered somewhere)
     * @throws NullPointerException if the packet is {@code null}
     */
    boolean transmit(Packet packet, ServerConnection connection);

    /**
     * Transmits a packet to a certain address.
     * <p>
     * If the connection to the target doesn't exist yet, the system will try to make the connection. In this case
     * true can be returned, even though the connection eventually can't be established.
     *
     * @param packet The Packet to transmit.
     * @param target The address of the target machine where the Packet should be transmitted.
     * @return true if the transmit was a success, false if a failure.
     * @throws NullPointerException if packet or target is null.
     * @see #transmit(Packet, ServerConnection)
     */
    boolean transmit(Packet packet, Address target);

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
}
