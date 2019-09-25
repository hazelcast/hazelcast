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

package com.hazelcast.internal.nio;

import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.networking.Networking;

import java.util.concurrent.TimeUnit;

/**
 * Networking service, it initializes the endpoints, the acceptor and the reactive networking stack
 * Given an {@link EndpointQualifier} an {@link EndpointManager} can be retrieved
 * by {@link #getEndpointManager(EndpointQualifier)} to create or get connections on that end.
 */
public interface NetworkingService<T extends Connection> {

    /**
     * Returns the I/O Service
     */
    IOService getIoService();

    /**
     * Returns the Networking engine
     * Shared across the multiple (or not) endpoints
     */
    Networking getNetworking();

    /**
     * Return an aggregate endpoint which acts as a view of all endpoints merged together for reporting purposes
     * eg. Read total-connections
     */
    AggregateEndpointManager getAggregateEndpointManager();

    /**
     * Returns the relevant {@link EndpointManager} given an {@link EndpointQualifier}
     * On single-endpoint setups (legacy mode), then a View relevant to the requested Endpoint is returned which purely acts
     * as a facade to hide the API differences and maintain common signatures.
     * eg.
     * {@link com.hazelcast.instance.ProtocolType#MEMBER} -&gt;
     * {@link com.hazelcast.internal.nio.tcp.MemberViewUnifiedEndpointManager}
     * {@link com.hazelcast.instance.ProtocolType#CLIENT} -&gt;
     * {@link com.hazelcast.internal.nio.tcp.ClientViewUnifiedEndpointManager}
     * {@link com.hazelcast.instance.ProtocolType#REST} -&gt;
     * {@link com.hazelcast.internal.nio.tcp.TextViewUnifiedEndpointManager}
     * {@link com.hazelcast.instance.ProtocolType#MEMCACHE} -&gt;
     * {@link com.hazelcast.internal.nio.tcp.TextViewUnifiedEndpointManager}
     */
    EndpointManager<T> getEndpointManager(EndpointQualifier qualifier);

    /**
     * Global scheduler for all Endpoints responsible of message retransmission
     */
    void scheduleDeferred(Runnable task, long delay, TimeUnit unit);

    /**
     * Flag indicating the liveness status of the Network
     */
    boolean isLive();

    /**
     * Starts the service, initializes its endpoints, starts threads, etc.
     * After start, Endpoints becomes fully operational.
     * <p>
     * If it is already started, then this method has no effect.
     *
     * @throws IllegalStateException if NetworkingService is shutdown
     */
    void start();

    /**
     * Stops the service, releases its resources, stops threads, etc.
     * When stopped, Endpoints can be started again using {@link #start()}.
     * <p>
     * This method has no effect if it is already stopped or shutdown.
     * <p>
     * Currently {@code stop} is called during the merge process to detach node from the current cluster. After
     * node becomes ready to join to the new cluster, {@code start} is called to re-initialize the Endpoints.
     */
    void stop();

    /**
     * Shutdowns the service completely.
     * Endpoints and the networking engine will not be operational anymore and cannot be restarted.
     * <p>
     * This method has no effect if it is already shutdown.
     */
    void shutdown();

}
