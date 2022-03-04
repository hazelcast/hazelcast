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

    public ClientConnectionStrategyConfig() {
    }

    public ClientConnectionStrategyConfig(ClientConnectionStrategyConfig config) {
        asyncStart = config.asyncStart;
        reconnectMode = config.reconnectMode;
        connectionRetryConfig = new ConnectionRetryConfig(config.connectionRetryConfig);
    }

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
     * @param reconnectMode the reconnect mode
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
     * @param connectionRetryConfig the connection retry config
     * @return the updated ClientConnectionStrategyConfig
     */
    public ClientConnectionStrategyConfig setConnectionRetryConfig(ConnectionRetryConfig connectionRetryConfig) {
        this.connectionRetryConfig = connectionRetryConfig;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientConnectionStrategyConfig that = (ClientConnectionStrategyConfig) o;

        if (asyncStart != that.asyncStart) {
            return false;
        }
        if (reconnectMode != that.reconnectMode) {
            return false;
        }
        return connectionRetryConfig != null
                ? connectionRetryConfig.equals(that.connectionRetryConfig) : that.connectionRetryConfig == null;
    }

    @Override
    public int hashCode() {
        int result = (asyncStart ? 1 : 0);
        result = 31 * result + (reconnectMode != null ? reconnectMode.hashCode() : 0);
        result = 31 * result + (connectionRetryConfig != null ? connectionRetryConfig.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ClientConnectionStrategyConfig{"
                + "asyncStart=" + asyncStart
                + ", reconnectMode=" + reconnectMode
                + ", connectionRetryConfig=" + connectionRetryConfig
                + '}';
    }
}
