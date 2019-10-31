/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.connection.nio.ClientConnection;
import com.hazelcast.client.impl.clientside.CandidateClusterContext;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListenable;

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;

/**
 * Responsible for managing {@link ClientConnection} objects.
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
     * @param address to be connected
     * @return connection if available, null otherwise
     */
    Connection getActiveConnection(Address address);

    /**
     * @param address to be connected
     * @return associated connection if available, creates new connection otherwise
     * @throws IOException if connection is not established
     */
    Connection getOrConnect(Address address) throws IOException;

    /**
     * @param address to be connected
     * @return associated connection if available, returns null and triggers new connection creation otherwise
     * @throws IOException if connection is not able to triggered
     */
    Connection getOrTriggerConnect(Address address) throws IOException;

    Collection<ClientConnection> getActiveConnections();

    UUID getClientUuid();

    void setCandidateClusterContext(CandidateClusterContext context);

    void beforeClusterSwitch(CandidateClusterContext context);
}
