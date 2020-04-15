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
import com.hazelcast.internal.server.tcp.ClientViewUnifiedEndpointManager;
import com.hazelcast.internal.server.tcp.MemberViewUnifiedServerConnectionManager;
import com.hazelcast.internal.server.tcp.TextViewUnifiedServerConnectionManager;

/**
 * The Server is responsible for managing {@link ServerConnection} instances.
 *
 * Given an {@link EndpointQualifier} an {@link ServerConnectionManager} can be retrieved
 * by {@link #getConnectionManager(EndpointQualifier)} to create or get connections on that end.
 */
public interface Server {

    /**
     * Returns the ServerContext.
     */
    ServerContext getContext();

    /**
     * Return an aggregate endpoint which acts as a view of all endpoints merged together for reporting purposes
     * eg. Read total-connections
     */
    AggregateServerConnectionManager getAggregateConnectionManager();

    /**
     * Returns the relevant {@link ServerConnectionManager} given an {@link EndpointQualifier}
     * On single-endpoint setups (legacy mode), then a View relevant to the requested Endpoint is returned which purely acts
     * as a facade to hide the API differences and maintain common signatures.
     * eg.
     * {@link com.hazelcast.instance.ProtocolType#MEMBER} -&gt;
     * {@link MemberViewUnifiedServerConnectionManager}
     * {@link com.hazelcast.instance.ProtocolType#CLIENT} -&gt;
     * {@link ClientViewUnifiedEndpointManager}
     * {@link com.hazelcast.instance.ProtocolType#REST} -&gt;
     * {@link TextViewUnifiedServerConnectionManager}
     * {@link com.hazelcast.instance.ProtocolType#MEMCACHE} -&gt;
     * {@link TextViewUnifiedServerConnectionManager}
     */
    ServerConnectionManager getConnectionManager(EndpointQualifier qualifier);

    /**
     * Flag indicating the liveness status of the Server
     */
    boolean isLive();

    /**
     * Starts the Server, initializes its endpoints, starts threads, etc.
     * After start, Endpoints becomes fully operational.
     * <p>
     * If it is already started, then this method has no effect.
     *
     * @throws IllegalStateException if NetworkingService is shutdown
     */
    void start();

    /**
     * Stops the Server, releases its resources, stops threads, etc.
     * When stopped, is can be started again by calling {@link #start()}.
     * <p>
     * This method has no effect if it is already stopped or shutdown.
     * <p>
     * Currently {@code stop} is called during the merge process to detach node from the current cluster. After
     * node becomes ready to join to the new cluster, {@code start} is called to re-initialize the Connections.
     */
    void stop();

    /**
     * Shutdowns the Server completely.
     * Connections and the networking engine will not be operational anymore and cannot be restarted.
     * <p>
     * This method has no effect if it is already shutdown.
     */
    void shutdown();

}
