/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.client.HazelcastClient;

/**
 * Client connection strategy configuration is used for setting custom strategies and configuring strategy parameters.
 */
public class ClientConnectionStrategyConfig {

    /**
     * Reconnect options.
     */
    public enum ReconnectMode {
        /**
         * Prevent reconnect to cluster after a disconnect
         */
        OFF,
        /**
         * Reconnect to cluster by blocking invocations
         */
        ON,
        /**
         * Reconnect to cluster without blocking invocations. Invocations will receive
         * {@link com.hazelcast.client.HazelcastClientOfflineException }
         */
        ASYNC
    }

    private boolean asyncStart;
    private ReconnectMode reconnectMode = ReconnectMode.ON;
    private ConnectionRetryConfig connectionRetryConfig = new ConnectionRetryConfig();

    /**
     * Client instance creation won't block on {@link HazelcastClient#newHazelcastClient()} if this value is true
     *
     * @return if client connects to cluster asynchronously
     */
    public boolean isAsyncStart() {
        return asyncStart;
    }

    /**
     * Set true for non blocking {@link HazelcastClient#newHazelcastClient()}. The client creation won't wait to
     * connect to cluster. The client instace will throw exception until it connects to cluster and become ready.
     * If set to false, {@link HazelcastClient#newHazelcastClient()} will block until a cluster connection established and it's
     * ready to use client instance
     *
     * default value is false
     *
     * @param asyncStart true for async client creation
     * @return the updated ClientConnectionStrategyConfig
     */
    public ClientConnectionStrategyConfig setAsyncStart(boolean asyncStart) {
        this.asyncStart = asyncStart;
        return this;
    }

    /**
     * @return reconnect mode
     */
    public ReconnectMode getReconnectMode() {
        return reconnectMode;
    }

    /**
     * How a client reconnect to cluster after a disconnect can be configured. This parameter is used by default strategy and
     * custom implementations may ignore it if configured.
     * default value is {@link ReconnectMode#ON}
     *
     * @param reconnectMode
     * @return the updated ClientConnectionStrategyConfig
     */
    public ClientConnectionStrategyConfig setReconnectMode(ReconnectMode reconnectMode) {
        this.reconnectMode = reconnectMode;
        return this;
    }

    /**
     * Connection Retry Config is controls the period among the retries and when should a client gave up
     * retrying. Exponential behaviour can be chosen or jitter can be added to wait periods.
     *
     * @return connection retry config
     */
    public ConnectionRetryConfig getConnectionRetryConfig() {
        return connectionRetryConfig;
    }

    /**
     * Connection Retry Config is controls the period among the retries and when should a client gave up
     * retrying. Exponential behaviour can be chosen or jitter can be added to wait periods.
     *
     * @param connectionRetryConfig
     * @return the updated ClientConnectionStrategyConfig
     */
    public ClientConnectionStrategyConfig setConnectionRetryConfig(ConnectionRetryConfig connectionRetryConfig) {
        this.connectionRetryConfig = connectionRetryConfig;
        return this;
    }
}
