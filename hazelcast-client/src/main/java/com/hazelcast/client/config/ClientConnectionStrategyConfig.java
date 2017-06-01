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

package com.hazelcast.client.config;

import com.hazelcast.client.connection.ClientConnectionStrategy;

import java.util.Properties;

/**
 * Client connection strategy configuration is used for setting custom strategies and configuring strategy parameters.
 */
public class ClientConnectionStrategyConfig {

    /**
     * Reconnect options
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

    private boolean clientStartAsync = true;

    private ReconnectMode reconnectMode = ReconnectMode.ON;

    private String className;

    private ClientConnectionStrategy implementation;

    private Properties properties = new Properties();

    /**
     *
     * @return
     */
    public boolean isClientStartAsync() {
        return clientStartAsync;
    }

    /**
     *
     * @param clientStartAsync
     * @return
     */
    public ClientConnectionStrategyConfig setClientStartAsync(boolean clientStartAsync) {
        this.clientStartAsync = clientStartAsync;
        return this;
    }

    /**
     *
     * @return
     */
    public ReconnectMode getReconnectMode() {
        return reconnectMode;
    }

    /**
     *
     * @param reconnectMode
     * @return
     */
    public ClientConnectionStrategyConfig setReconnectMode(ReconnectMode reconnectMode) {
        this.reconnectMode = reconnectMode;
        return this;
    }

    /**
     *
     * @return
     */
    public String getClassName() {
        return className;
    }

    /**
     *
     * @param className
     * @return
     */
    public ClientConnectionStrategyConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    /**
     *
     * @return
     */
    public ClientConnectionStrategy getImplementation() {
        return implementation;
    }

    /**
     *
     * @param implementation
     * @return
     */
    public ClientConnectionStrategyConfig setImplementation(ClientConnectionStrategy implementation) {
        this.implementation = implementation;
        return this;
    }

    /**
     * Sets a property.
     *
     * @param name  the name of the property to set
     * @param value the value of the property to set
     * @return the updated ClientConnectionStrategyConfig
     * @throws NullPointerException if name or value is {@code null}
     */
    public ClientConnectionStrategyConfig setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    /**
     * Gets a property.
     *
     * @param name the name of the property to get
     * @return the value of the property, null if not found
     * @throws NullPointerException if name is {@code null}
     */
    public String getProperty(String name) {
        return properties.getProperty(name);
    }

    /**
     * Gets all properties.
     *
     * @return the properties
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the properties.
     *
     * @param properties the properties to set
     * @return the updated ClientConnectionStrategyConfig
     * @throws IllegalArgumentException if properties is {@code null}
     */
    public ClientConnectionStrategyConfig setProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties can't be null");
        }
        this.properties = properties;
        return this;
    }

}
