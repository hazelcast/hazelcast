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

package com.hazelcast.client;

import com.hazelcast.cluster.Endpoint;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.UUID;

/**
 * The Client interface allows to get information about
 * a connected client's socket address, type and UUID.
 *
 * @see ClientService
 * @see ClientListener
 */
public interface Client extends Endpoint {

    /**
     * Returns a unique UUID for this client.
     *
     * @return a unique UUID for this client
     */
    UUID getUuid();

    /**
     * Returns the socket address of this client.
     *
     * @return the socket address of this client
     */
    InetSocketAddress getSocketAddress();

    /**
     * Type could be a client type from {@link com.hazelcast.internal.nio.ConnectionType} or
     * it can be a custom client implementation with a name outside of this @{link ConnectionType}
     *
     * @return the type of this client
     */
    String getClientType();


    /**
     * This method may return null depending on the client version and the client type
     * Java client provides client name starting with 3.12
     *
     * @return the name of this client if provided, null otherwise
     * @since 3.12
     */
    String getName();

    /**
     * @return read only set of all labels of this client.
     * @since 3.12
     */
    Set<String> getLabels();
}
