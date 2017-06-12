/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.connection;

import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;

/**
 * An abstract class called from {@link ClientConnectionManager} to customize
 * how client connect to cluster, and provide various behaviours like async start or restart.
 */
public abstract class ClientConnectionStrategy {

    protected ClientContext clientContext;
    protected ILogger logger;
    protected ClientConnectionStrategyConfig clientConnectionStrategyConfig;

    public ClientConnectionStrategy() {
    }

    /**
     * Initialize this strategy with client context and config
     * @param clientContext hazelcast client context to access internal services
     */
    public final void init(ClientContext clientContext) {
        this.clientContext = clientContext;
        this.clientConnectionStrategyConfig = clientContext.getClientConfig().getConnectionStrategyConfig();
        this.logger = clientContext.getLoggingService().getLogger(ClientConnectionStrategy.class);
    }

    /**
     * Called after {@link ClientConnectionManager} started.
     * Connecting to cluster can be triggered from this method using one of
     * {@link ClientConnectionManager#connectToCluster} or
     * {@link ClientConnectionManager#connectToClusterAsync}
     */
    public abstract void start();

    /**
     * The purpose of this method is to validate a connection request by target, and exit the blocking invocation.
     * For all connection requests on {@link ClientConnectionManager} this method will be called.
     *
     * The build in retry mechanism can be stopped by throwing an instance of non retryable exceptions;
     * {@link java.io.IOException}, {@link com.hazelcast.core.HazelcastInstanceNotActiveException} or
     * {@link com.hazelcast.spi.exception.RetryableException}
     *
     * The thrown exception will be received on the blocking user. Any blocking invocation will exit by that exception.
     * @param target address of the requested connection
     */
    public abstract void beforeGetConnection(Address target);

    /**
     * If a new connection is required to open by {@link ClientConnectionManager},
     * this method will be called.
     *
     * This request can be rejected by throwing an instance of non retryable exceptions;
     * {@link java.io.IOException}, {@link com.hazelcast.core.HazelcastInstanceNotActiveException} or
     * {@link com.hazelcast.spi.exception.RetryableException}
     * @param target address of the requested connection
     */
    public abstract void beforeOpenConnection(Address target);

    /**
     * If a cluster connection is established, this method will be called.
     * if an exception is thrown, the already established connection will be closed.
     */
    public abstract void onConnectToCluster();

    /**
     * If the cluster connection is lost for any reason, this method will be called.
     *
     */
    public abstract void onDisconnectFromCluster();

    /**
     * If the {@link ClientConnectionManager} opens a new connection to a member,
     * this method will be called with the connection parameter
     * @param connection the new established connection
     */
    public abstract void onConnect(ClientConnection connection);

    /**
     * If a connection is disconnected, this method will be called with the connection parameter
     * @param connection the closed connection
     */
    public abstract void onDisconnect(ClientConnection connection);

    /**
     * The {@link ClientConnectionManager} will inform this method that the provided connection's heartbeat stopped
     * @param connection the connection that heartbeat failed
     */
    public abstract void onHeartbeatStopped(ClientConnection connection);

    /**
     * The {@link ClientConnectionManager} will inform this method that the provided connection's heartbeat resumed
     * @param connection the connection that heartbeat resumed
     */
    public abstract void onHeartbeatResumed(ClientConnection connection);

    /**
     * The {@link ClientConnectionManager} will call this method as a last step of its shutdown.
     */
    public abstract void shutdown();

}
