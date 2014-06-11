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

import com.hazelcast.util.ValidationUtil;

import java.util.Properties;

/**
 * MapStore configuration.
 */
public class MapStoreConfig {
    public static final int DEFAULT_WRITE_DELAY_SECONDS = 0;

    private boolean enabled = true;
    private String className;
    private String factoryClassName;
    private int writeDelaySeconds = DEFAULT_WRITE_DELAY_SECONDS;
    private Object implementation;
    private Object factoryImplementation;
    private Properties properties = new Properties();
    private MapStoreConfigReadOnly readOnly;
    private InitialLoadMode initialLoadMode = InitialLoadMode.LAZY;

    public enum InitialLoadMode {
        LAZY, EAGER
    }


    public MapStoreConfig() {
    }

    public MapStoreConfig(MapStoreConfig config) {
        enabled = config.isEnabled();
        className = config.getClassName();
        implementation = config.getImplementation();
        factoryClassName = config.getFactoryClassName();
        factoryImplementation = config.getFactoryImplementation();
        writeDelaySeconds = config.getWriteDelaySeconds();
        initialLoadMode = config.getInitialLoadMode();
        properties.putAll(config.getProperties());
    }

    public MapStoreConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MapStoreConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Returns the name of the MapStore implementation class
     *
     * @return the name of the class
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the name for the MapStore implementation class
     *
     * @param className the name of the MapStore implementation class to set
     */
    public MapStoreConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    /**
     * Returns the name of the MapStoreFactory implementation class
     *
     * @return the name of the class
     */
    public String getFactoryClassName() {
        return factoryClassName;
    }

    /**
     * Sets the name for the MapStoreFactory implementation class
     *
     * @param factoryClassName the name of the MapStoreFactory implementation class to set
     */
    public MapStoreConfig setFactoryClassName(String factoryClassName) {
        this.factoryClassName = factoryClassName;
        return this;
    }

    /**
     * Returns the number of seconds to delay the store writes.
     *
     * @return the number of delay seconds
     */
    public int getWriteDelaySeconds() {
        return writeDelaySeconds;
    }

    /**
     * Sets the number of seconds to delay before writing (storing) the dirty records
     *
     * @param writeDelaySeconds the number of seconds to delay
     */
    public MapStoreConfig setWriteDelaySeconds(int writeDelaySeconds) {
        this.writeDelaySeconds = writeDelaySeconds;
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
    public MapStoreConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Sets the map store implementation object
     *
     * @param implementation implementation object
     * @return this MapStoreConfig instance
     */
    public MapStoreConfig setImplementation(Object implementation) {
        this.implementation = implementation;
        return this;
    }

    /**
     * Returns the map store implementation object
     *
     * @return map store implementation object
     */
    public Object getImplementation() {
        return implementation;
    }

    /**
     * Sets the map store factory implementation object
     *
     * @param factoryImplementation factory implementation object
     * @return this MapStoreConfig instance
     */
    public MapStoreConfig setFactoryImplementation(Object factoryImplementation) {
        this.factoryImplementation = factoryImplementation;
        return this;
    }

    /**
     * Returns the map store factory implementation object
     *
     * @return map store factory implementation object
     */
    public Object getFactoryImplementation() {
        return factoryImplementation;
    }

    public MapStoreConfig setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    public String getProperty(String name) {
        return properties.getProperty(name);
    }

    public Properties getProperties() {
        return properties;
    }

    public MapStoreConfig setProperties(Properties properties) {
        ValidationUtil.isNotNull(properties, "properties");
        this.properties = properties;
        return this;
    }

    /**
     * Returns initial load mode
     *
     * @return initial load mode object
     */
    public InitialLoadMode getInitialLoadMode() {
        return initialLoadMode;
    }

    /**
     * Sets initial load mode
     * <p/>
     * LAZY: Default load mode where load is async
     * EAGER: load mode where load is blocked till all partitions are loaded
     *
     * @param initialLoadMode initial load mode object
     */
    public MapStoreConfig setInitialLoadMode(InitialLoadMode initialLoadMode) {
        this.initialLoadMode = initialLoadMode;
        return this;
    }


    @Override
    public String toString() {
        return "MapStoreConfig{"
                + "className='" + className
                + '\''
                + ", enabled=" + enabled
                + ", writeDelaySeconds=" + writeDelaySeconds
                + ", implementation=" + implementation
                + ", properties=" + properties
                + ", initialLoadMode=" + initialLoadMode
                + '}';
    }
}
