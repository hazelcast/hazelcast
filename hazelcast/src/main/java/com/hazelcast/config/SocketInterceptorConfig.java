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

package com.hazelcast.config;

import com.hazelcast.nio.SocketInterceptor;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

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
     * Returns the name of the {@link SocketInterceptor} implementation class.
     *
     * @return name of the {@link SocketInterceptor} implementation class
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the name for the {@link SocketInterceptor} implementation class.
     *
     * @param className the name of the {@link SocketInterceptor} implementation class to set
     * @return this SocketInterceptorConfig instance
     */
    public SocketInterceptorConfig setClassName(@Nonnull String className) {
        this.className = checkHasText(className, "Socket interceptor class name must contain text");
        this.implementation = null;
        return this;
    }

    /**
     * Sets the {@link SocketInterceptor} implementation object.
     *
     * @param implementation the {@link SocketInterceptor} implementation object to set
     * @return this SocketInterceptorConfig instance
     */
    public SocketInterceptorConfig setImplementation(@Nonnull Object implementation) {
        this.implementation = checkNotNull(implementation, "Socket interceptor cannot be null!");
        this.className = null;
        return this;
    }

    /**
     * Returns the {@link SocketInterceptor} implementation object.
     *
     * @return the {@link SocketInterceptor} implementation object
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
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof SocketInterceptorConfig)) {
            return false;
        }

        SocketInterceptorConfig that = (SocketInterceptorConfig) o;

        return enabled == that.enabled
            && Objects.equals(implementation, that.implementation)
            && Objects.equals(className, that.className)
            && Objects.equals(properties, that.properties);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(enabled, implementation, className, properties);
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
