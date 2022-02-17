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

package com.hazelcast.client.impl;

import com.hazelcast.internal.nio.Connection;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;

/**
 * A manager for {@link com.hazelcast.client.impl.ClientEndpoint}s.
 *
 * All the methods are thread-safe.
 */
public interface ClientEndpointManager {

    /**
     * Returns the current endpoints.
     *
     * @return the endpoints.
     */
    Collection<ClientEndpoint> getEndpoints();

    /**
     * Gets the endpoint for a given connection.
     *
     * @param connection the connection to the endpoint.
     * @return the found endpoint or null of no endpoint was found.
     * @throws java.lang.NullPointerException if connection is null.
     */
    ClientEndpoint getEndpoint(Connection connection);

    /**
     * Gets all the endpoints for a given client.
     *
     * @return a set of all the endpoints for the client. If no endpoints are found, an empty set is returned.
     * @throws java.lang.NullPointerException if clientUuid is null.
     */
    Set<UUID> getLocalClientUuids();

    /**
     * Returns the current number of endpoints.
     *
     * @return the current number of endpoints.
     */
    int size();

    /**
     * Removes all endpoints. Nothing is done on the endpoints themselves; they are just removed from this ClientEndpointManager.
     *
     * Can safely be called when there are no endpoints.
     */
    void clear();

    /**
     * Registers an endpoint with this ClientEndpointManager.
     *
     * If the endpoint already is registered, the call is ignored.
     *
     * @param endpoint the endpoint to register.
     * @return false if an endpoint is already registered
     * @throws java.lang.NullPointerException if endpoint is null.
     */
    boolean registerEndpoint(ClientEndpoint endpoint);

    /**
     * Removes an endpoint from this ClientEndpointManager.
     * Closes associated connection and runs all registered destroy actions to endpoint.
     *
     * No action taken if endpoint is not registered or already removed
     *
     * @param endpoint the endpoint to remove.
     * @throws java.lang.NullPointerException if endpoint is null.
     */
    void removeEndpoint(ClientEndpoint endpoint);

}
