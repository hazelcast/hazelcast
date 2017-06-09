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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.connection.ClientConnectionStrategy;

import java.util.Properties;

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

    private boolean asyncStart = false;

    private ReconnectMode reconnectMode = ReconnectMode.ON;

    private String className;

    private ClientConnectionStrategy implementation;

    private Properties properties = new Properties();

    /**
     * Client instance creation won't block on {@link HazelcastClient#newHazelcastClient()} if this value is true
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
     * @param reconnectMode
     * @return
     */
    public ClientConnectionStrategyConfig setReconnectMode(ReconnectMode reconnectMode) {
        this.reconnectMode = reconnectMode;
        return this;
    }

    /**
     * @see ClientConnectionStrategy
     * @return class name of client connection strategy implementation
     */
    public String getClassName() {
        return className;
    }

    /**
     * Class name of the strategy implementation where it should be a subclass of {@link ClientConnectionStrategy}
     * @param className class name of client connection strategy implementation
     * @return the updated ClientConnectionStrategyConfig
     */
    public ClientConnectionStrategyConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    /**
     * An instance of the strategy implementation where it should be a subclass of {@link ClientConnectionStrategy}
     * @return configured strategy implementation instance or null if none configured
     */
    public ClientConnectionStrategy getImplementation() {
        return implementation;
    }

    /**
     * A user created instance can be used to configure the client connection strategy.
     * @param implementation An instance of the strategy implementation
     *                      where it should be a subclass of {@link ClientConnectionStrategy}
     * @return the updated ClientConnectionStrategyConfig
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
     * These properties are populated into {@link ClientConnectionStrategy} to be used in implementations of it.
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
