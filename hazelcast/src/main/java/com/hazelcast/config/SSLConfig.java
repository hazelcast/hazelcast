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
 * SSL configuration
 */
public final class SSLConfig {

    private boolean enabled;
    private String factoryClassName;
    private Object factoryImplementation;
    private Properties properties = new Properties();

    /**
     * Returns the name of the {@link com.hazelcast.nio.ssl.SSLContextFactory} implementation class
     *
     * @return the name of the class
     */
    public String getFactoryClassName() {
        return factoryClassName;
    }

    /**
     * Sets the name for the {@link com.hazelcast.nio.ssl.SSLContextFactory} implementation class
     *
     * @param factoryClassName the name of the {@link com.hazelcast.nio.ssl.SSLContextFactory} implementation class to set
     */
    public SSLConfig setFactoryClassName(String factoryClassName) {
        this.factoryClassName = factoryClassName;
        return this;
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
    public SSLConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Sets the {@link com.hazelcast.nio.ssl.SSLContextFactory} implementation object
     *
     * @param factoryImplementation factory implementation object
     * @return this SSLConfig instance
     */
    public SSLConfig setFactoryImplementation(Object factoryImplementation) {
        this.factoryImplementation = factoryImplementation;
        return this;
    }

    /**
     * Returns the {@link com.hazelcast.nio.ssl.SSLContextFactory} implementation object
     *
     * @return {@link com.hazelcast.nio.ssl.SSLContextFactory} implementation object
     */
    public Object getFactoryImplementation() {
        return factoryImplementation;
    }

    /**
     * Sets a property.
     *
     * @param name  the name of the property to set.
     * @param value the value of the property to set
     * @return the updated SSLConfig
     * @throws NullPointerException if name or value is null.
     */
    public SSLConfig setProperty(String name, String value) {
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
}
