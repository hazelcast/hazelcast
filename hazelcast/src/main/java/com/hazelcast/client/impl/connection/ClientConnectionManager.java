/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.connection;

import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.client.impl.connection.tcp.RoutingMode;
import com.hazelcast.client.impl.management.ClientConnectionProcessListener;
import com.hazelcast.internal.nio.ConnectionListenable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.UUID;

/**
 * Responsible for managing {@link ClientConnection}.
 */
public interface ClientConnectionManager extends ConnectionListenable<ClientConnection> {

    /**
     * Check if ClientConnectionManager is alive.
     * ClientConnectionManager is not alive only when client is closing.
     *
     * @return true if alive, false otherwise.
     */
    boolean isAlive();

    /**
     * @param uuid UUID of the member to get connection of
     * @return connection if available, null otherwise
     */
    ClientConnection getActiveConnection(@Nonnull UUID uuid);

    /**
     * Check the connected state and user connection strategy configuration to see if an invocation is allowed at the moment
     * returns without throwing exception only when is the client is Connected to cluster
     *
     * @throws IOException                     if client is disconnected and ReconnectMode is ON or
     *                                         if client is starting and async start is false
     * @throws HazelcastClientOfflineException if client is disconnected and ReconnectMode is ASYNC or
     *                                         if client is starting and async start is true
     */
    void checkInvocationAllowed() throws IOException;

    /**
     * Returns {@code true} if the client is initialized on the cluster, by
     * sending its local state, if necessary.
     */
    boolean clientInitializedOnCluster();

    Collection<ClientConnection> getActiveConnections();

    UUID getClientUuid();

    /**
     * For an {@link RoutingMode#ALL_MEMBERS} or {@link RoutingMode#MULTI_MEMBER} routing client, a random
     * ClientConnection is chosen via LoadBalancer.
     * For a {@link RoutingMode#SINGLE_MEMBER} routing client the only ClientConnection will be returned.
     *
     * @return random ClientConnection if available, null otherwise
     */
    ClientConnection getRandomConnection();

    /**
     * Return:<ol>
     * <li>a random connection to a data member from the larger same-version
     * group
     * <li>if there's no such connection, return connection to a random data
     * member
     * <li>if there's no such connection, return any random connection
     * </ol>
     */
    ClientConnection getConnectionForSql();

    String getConnectionType();

    void addClientConnectionProcessListener(ClientConnectionProcessListener listener);

    /**
     * <lu>
     * <li>SINGLE_MEMBER: routing can only be done to the single connected member</li>
     * <li>ALL_MEMBERS: routing can be done to all members by knowing where data is</li>
     * <li>MULTI_MEMBER: routing can be done based on a defined grouping strategy</li>
     * </lu>
     */
    RoutingMode getRoutingMode();
}
