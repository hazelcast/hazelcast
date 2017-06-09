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
 *
 */
public abstract class ClientConnectionStrategy {

    protected ClientContext clientContext;
    protected ILogger logger;
    protected ClientConnectionStrategyConfig clientConnectionStrategyConfig;
    public ClientConnectionStrategy() {
    }

    public final void init(ClientContext clientContext) {
        this.clientContext = clientContext;
        this.clientConnectionStrategyConfig = clientContext.getClientConfig().getConnectionStrategyConfig();
        this.logger = clientContext.getLoggingService().getLogger(ClientConnectionStrategy.class);
    }

    /**
     */
    public abstract void init();

    /**
     * @param target
     */
    public abstract void beforeGetConnection(Address target);

    /**
     * @param target
     */
    public abstract void beforeOpenConnection(Address target);

    /**
     */
    public abstract void onConnectToCluster();

    /**
     *
     */
    public abstract void onDisconnectFromCluster();

    /**
     * @param connection
     */
    public abstract void onConnect(ClientConnection connection);

    /**
     * @param connection
     */
    public abstract void onDisconnect(ClientConnection connection);

    /**
     * @param connection
     */
    public abstract void onHeartbeatStopped(ClientConnection connection);

    /**
     * @param connection
     */
    public abstract void onHeartbeatResumed(ClientConnection connection);

    /**
     *
     */
    public void shutdown() {
    }

}
