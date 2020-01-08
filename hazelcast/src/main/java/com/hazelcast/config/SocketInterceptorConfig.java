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

package com.hazelcast.config;

import java.util.Properties;

/**
 * Contains the configuration for interceptor socket.
 */
public class SocketInterceptorConfig {
    private boolean enabled;
    private String className;
    private Object implementation;
    private Properties properties = new Properties();

    public SocketInterceptorConfig() {
    }

    public SocketInterceptorConfig(SocketInterceptorConfig socketInterceptorConfig) {
        enabled = socketInterceptorConfig.enabled;
        className = socketInterceptorConfig.className;
        implementation = socketInterceptorConfig.implementation;
        properties = new Properties();
        properties.putAll(socketInterceptorConfig.properties);
    }

    /**
     * Returns the name of the {@link com.hazelcast.nio.SocketInterceptor} implementation class.
     *
     * @return name of the {@link com.hazelcast.nio.SocketInterceptor} implementation class
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the name for the {@link com.hazelcast.nio.SocketInterceptor} implementation class.
     *
     * @param className the name of the {@link com.hazelcast.nio.SocketInterceptor} implementation class to set
     * @return this SocketInterceptorConfig instance
     */
    public SocketInterceptorConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    /**
     * Sets the {@link com.hazelcast.nio.SocketInterceptor} implementation object.
     *
     * @param implementation the {@link com.hazelcast.nio.SocketInterceptor} implementation object to set
     * @return this SocketInterceptorConfig instance
     */
    public SocketInterceptorConfig setImplementation(Object implementation) {
        this.implementation = implementation;
        return this;
    }

    /**
     * Returns the {@link com.hazelcast.nio.SocketInterceptor} implementation object.
     *
     * @return the {@link com.hazelcast.nio.SocketInterceptor} implementation object
     */
    public Object getImplementation() {
        return implementation;
    }

    /**
     * Returns if this configuration is enabled.
     *
     * @return {@code true} if enabled, {@code false} otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables and disables this configuration.
     *
     * @param enabled true to enable, false to disable
     */
    public SocketInterceptorConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Sets a property.
     *
     * @param name  the name of the property to set
     * @param value the value of the property to set
     * @return the updated SocketInterceptorConfig
     * @throws NullPointerException if name or value is {@code null}
     */
    public SocketInterceptorConfig setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    /**
     * Gets a property.
     *
     * @param name the name of the property to get
     * @return the value of the property, {@code null} if not found
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
     * @return the updated SSLConfig
     * @throws IllegalArgumentException if properties is {@code null}
     */
    public SocketInterceptorConfig setProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties can't be null");
        }

        this.properties = properties;
        return this;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof SocketInterceptorConfig)) {
            return false;
        }

        SocketInterceptorConfig that = (SocketInterceptorConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (className != null ? !className.equals(that.className) : that.className != null) {
            return false;
        }
        if (implementation != null ? !implementation.equals(that.implementation) : that.implementation != null) {
            return false;
        }
        return properties != null ? properties.equals(that.properties) : that.properties == null;
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (className != null ? className.hashCode() : 0);
        result = 31 * result + (implementation != null ? implementation.hashCode() : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SocketInterceptorConfig{"
                + "className='" + className + '\''
                + ", enabled=" + enabled
                + ", implementation=" + implementation
                + ", properties=" + properties
                + '}';
    }
}
