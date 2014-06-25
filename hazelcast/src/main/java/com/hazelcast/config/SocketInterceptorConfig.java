/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import java.util.Properties;

/**
 * Contains the configuration for interceptor socket
 */
public class SocketInterceptorConfig {
    private boolean enabled;
    private String className;
    private Object implementation;
    private Properties properties = new Properties();

    /**
     * Returns the name of the {@link com.hazelcast.nio.SocketInterceptor} implementation class
     *
     * @return name of the class
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the name for the {@link com.hazelcast.nio.SocketInterceptor} implementation class
     *
     * @param className the name of the {@link com.hazelcast.nio.SocketInterceptor} implementation class to set
     * @return this SocketInterceptorConfig instance
     */
    public SocketInterceptorConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    /**
     * Sets the {@link com.hazelcast.nio.SocketInterceptor} implementation object
     *
     * @param implementation implementation object
     * @return this SocketInterceptorConfig instance
     */
    public SocketInterceptorConfig setImplementation(Object implementation) {
        this.implementation = implementation;
        return this;
    }

    /**
     * Returns the {@link com.hazelcast.nio.SocketInterceptor} implementation object
     *
     * @return SocketInterceptor implementation object
     */
    public Object getImplementation() {
        return implementation;
    }

    /**
     * Returns if this configuration is enabled
     *
     * @return true if enabled, false otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables and disables this configuration
     *
     * @param enabled
     */
    public SocketInterceptorConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Sets a property.
     *
     * @param name  the name of the property to set.
     * @param value the value of the property to set
     * @return the updated SocketInterceptorConfig
     * @throws NullPointerException if name or value is null.
     */
    public SocketInterceptorConfig setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    /**
     * Gets a property.
     *
     * @param name the name of the property to get.
     * @return the value of the property, null if not found
     * @throws NullPointerException if name is null.
     */
    public String getProperty(String name) {
        return properties.getProperty(name);
    }

    /**
     * Gets all properties.
     *
     * @return the properties.
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the properties.
     *
     * @param properties the properties to set.
     * @return the updated SSLConfig.
     * @throws IllegalArgumentException if properties is null.
     */
    public SocketInterceptorConfig setProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties can't be null");
        }

        this.properties = properties;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("SocketInterceptorConfig");
        sb.append("{className='").append(className).append('\'');
        sb.append(", enabled=").append(enabled);
        sb.append(", implementation=").append(implementation);
        sb.append(", properties=").append(properties);
        sb.append('}');
        return sb.toString();
    }
}
