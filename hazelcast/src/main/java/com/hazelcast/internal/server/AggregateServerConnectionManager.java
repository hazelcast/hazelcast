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

import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.nio.ConnectionListenable;

import java.util.Collection;
import java.util.Map;

public interface AggregateServerConnectionManager
        extends ConnectionListenable {

    /**
     * Returns all connections that have been successfully established by the underlying EndpointManagers.
     *
     * @return active connections
     */
    Collection<ServerConnection> getConnections();

    /**
     * Returns all active connections from the underlying EndpointManagers.
     *
     * @return active connections
     */
    Collection<ServerConnection> getActiveConnections();

    /**
     * Returns network stats for inbound and outbound traffic per {@link EndpointQualifier}.
     * Stats are available only when Advanced Networking is enabled.
     *
     * @return network stats per endpoint
     */
    Map<EndpointQualifier, NetworkStats> getNetworkStats();

}
