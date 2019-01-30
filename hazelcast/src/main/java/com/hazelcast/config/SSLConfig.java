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

package com.hazelcast.config;

import java.util.Properties;

/**
 * SSL configuration.
 */
public final class SSLConfig {

    private boolean enabled;
    private String factoryClassName;
    private Object factoryImplementation;
    private Properties properties = new Properties();

    public SSLConfig() {
    }

    public SSLConfig(SSLConfig sslConfig) {
        enabled = sslConfig.enabled;
        factoryClassName = sslConfig.factoryClassName;
        factoryImplementation = sslConfig.factoryImplementation;
        properties = new Properties();
        properties.putAll(sslConfig.properties);
    }

    /**
     * Returns the name of the implementation class.
     * <p>
     * Class can either be an  {@link com.hazelcast.nio.ssl.SSLContextFactory} or {@link com.hazelcast.nio.ssl.SSLEngineFactory}.
     *
     * @return the name implementation class
     */
    public String getFactoryClassName() {
        return factoryClassName;
    }

    /**
     * Sets the name for the implementation class.
     * <p>
     * Class can either be an  {@link com.hazelcast.nio.ssl.SSLContextFactory} or {@link com.hazelcast.nio.ssl.SSLEngineFactory}.
     *
     * @param factoryClassName the name implementation class
     */
    public SSLConfig setFactoryClassName(String factoryClassName) {
        this.factoryClassName = factoryClassName;
        return this;
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
     * @param enabled {@code true} to enable, {@code false} to disable
     */
    public SSLConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Sets the implementation object.
     * <p>
     * Object must be instance of an {@link com.hazelcast.nio.ssl.SSLContextFactory} or
     * {@link com.hazelcast.nio.ssl.SSLEngineFactory}.
     *
     * @param factoryImplementation the implementation object
     * @return this SSLConfig instance
     */
    public SSLConfig setFactoryImplementation(Object factoryImplementation) {
        this.factoryImplementation = factoryImplementation;
        return this;
    }

    /**
     * Returns the factory implementation object.
     * <p>
     * Object is instance of an {@link com.hazelcast.nio.ssl.SSLContextFactory} or {@link com.hazelcast.nio.ssl.SSLEngineFactory}.
     *
     * @return the factory implementation object
     */
    public Object getFactoryImplementation() {
        return factoryImplementation;
    }

    /**
     * Sets a property.
     *
     * @param name  the name of the property to set
     * @param value the value of the property to set
     * @return the updated SSLConfig
     * @throws NullPointerException if name or value is {@code null}
     */
    public SSLConfig setProperty(String name, String value) {
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
     * @return the updated SSLConfig
     * @throws IllegalArgumentException if properties is {@code null}
     */
    public SSLConfig setProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties can't be null");
        }
        this.properties = properties;
        return this;
    }

    @Override
    public String toString() {
        return "SSLConfig{"
                + "className='" + factoryClassName + '\''
                + ", enabled=" + enabled
                + ", implementation=" + factoryImplementation
                + ", properties=" + properties
                + '}';
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SSLConfig sslConfig = (SSLConfig) o;

        if (enabled != sslConfig.enabled) {
            return false;
        }
        if (factoryClassName != null
                ? !factoryClassName.equals(sslConfig.factoryClassName) : sslConfig.factoryClassName != null) {
            return false;
        }
        if (factoryImplementation != null
                ? !factoryImplementation.equals(sslConfig.factoryImplementation) : sslConfig.factoryImplementation != null) {
            return false;
        }
        return properties != null ? properties.equals(sslConfig.properties) : sslConfig.properties == null;
    }

    @Override
    public int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (factoryClassName != null ? factoryClassName.hashCode() : 0);
        result = 31 * result + (factoryImplementation != null ? factoryImplementation.hashCode() : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        return result;
    }
}
