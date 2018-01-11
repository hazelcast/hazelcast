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

package com.hazelcast.config;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.properties.GroupProperty;

import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains the configuration for a Map Store.
 */
public class MapStoreConfig implements IdentifiedDataSerializable {
    /**
     * Default delay seconds for writing
     */
    public static final int DEFAULT_WRITE_DELAY_SECONDS = 0;
    /**
     * Default batch size for writing
     */
    public static final int DEFAULT_WRITE_BATCH_SIZE = 1;
    /**
     * Default write coalescing behavior
     */
    public static final boolean DEFAULT_WRITE_COALESCING = true;

    private boolean enabled = true;
    private boolean writeCoalescing = DEFAULT_WRITE_COALESCING;
    private String className;
    private String factoryClassName;
    private int writeDelaySeconds = DEFAULT_WRITE_DELAY_SECONDS;
    private int writeBatchSize = DEFAULT_WRITE_BATCH_SIZE;
    private Object implementation;
    private Object factoryImplementation;
    private Properties properties = new Properties();
    private transient MapStoreConfigReadOnly readOnly;
    private InitialLoadMode initialLoadMode = InitialLoadMode.LAZY;

    /**
     * Initial load module
     */
    public enum InitialLoadMode {
        /**
         * Each partition is loaded when it is first touched.
         */
        LAZY,
        /**
         * getMap() method does not return till the map is completely loaded.
         */
        EAGER
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
        writeBatchSize = config.getWriteBatchSize();
        initialLoadMode = config.getInitialLoadMode();
        writeCoalescing = config.isWriteCoalescing();
        properties.putAll(config.getProperties());
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public MapStoreConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MapStoreConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Returns the name of the MapStore implementation class
     *
     * @return the name of the MapStore implementation class
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the name for the MapStore implementation class
     *
     * @param className the name to set for the MapStore implementation class
     */
    public MapStoreConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    /**
     * Returns the name of the MapStoreFactory implementation class
     *
     * @return the name of the MapStoreFactory implementation class
     */
    public String getFactoryClassName() {
        return factoryClassName;
    }

    /**
     * Sets the name for the MapStoreFactory implementation class
     *
     * @param factoryClassName the name to set for the MapStoreFactory implementation class
     */
    public MapStoreConfig setFactoryClassName(String factoryClassName) {
        this.factoryClassName = factoryClassName;
        return this;
    }

    /**
     * Returns the number of seconds to delay the store writes.
     *
     * @return the number of seconds to delay the store writes
     */
    public int getWriteDelaySeconds() {
        return writeDelaySeconds;
    }

    /**
     * Sets the number of seconds to delay before writing (storing) the dirty records
     * <p>
     * Default value is {@value #DEFAULT_WRITE_DELAY_SECONDS}.
     *
     * @param writeDelaySeconds the number of seconds to delay before writing the dirty records
     */
    public MapStoreConfig setWriteDelaySeconds(int writeDelaySeconds) {
        this.writeDelaySeconds = writeDelaySeconds;
        return this;
    }

    /**
     * Returns the number of operations to be included in each batch processing round.
     *
     * @return write batch size: the number of operations to be included in each batch processing round
     */
    public int getWriteBatchSize() {
        return writeBatchSize;
    }

    /**
     * Sets the number of operations to be included in each batch processing round.
     * <p>
     * Default value is {@value #DEFAULT_WRITE_BATCH_SIZE}.
     *
     * @param writeBatchSize the number of operations to be included in each batch processing round
     */
    public MapStoreConfig setWriteBatchSize(int writeBatchSize) {
        if (writeBatchSize < 1) {
            throw new IllegalArgumentException("Write batch size should be at least 1");
        }
        this.writeBatchSize = writeBatchSize;
        return this;
    }

    /**
     * Returns if this configuration is enabled
     *
     * @return {@code true} if this configuration is enabled, {@code false} otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables and disables this configuration
     *
     * @param enabled {@code true} to enables this configuration, {@code false} to disable
     */
    public MapStoreConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Sets the map store implementation object
     *
     * @param implementation the map store implementation object to set
     * @return this MapStoreConfig instance
     */
    public MapStoreConfig setImplementation(Object implementation) {
        this.implementation = implementation;
        return this;
    }

    /**
     * Returns the map store implementation object.
     *
     * @return the map store implementation object
     */
    public Object getImplementation() {
        return implementation;
    }

    /**
     * Sets the map store factory implementation object.
     *
     * @param factoryImplementation the factory implementation object
     * @return this MapStoreConfig instance
     */
    public MapStoreConfig setFactoryImplementation(Object factoryImplementation) {
        this.factoryImplementation = factoryImplementation;
        return this;
    }

    /**
     * Returns the map store factory implementation object.
     *
     * @return the map store factory implementation object
     */
    public Object getFactoryImplementation() {
        return factoryImplementation;
    }

    public MapStoreConfig setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    /**
     * Returns the given property
     *
     * @return the given property
     */
    public String getProperty(String name) {
        return properties.getProperty(name);
    }

    /**
     * Returns all the properties
     *
     * @return all the properties
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the properties
     *
     * @param properties the properties to be set
     * @return this MapStoreConfig
     */
    public MapStoreConfig setProperties(Properties properties) {
        this.properties = isNotNull(properties, "properties");
        return this;
    }

    /**
     * Returns the initial load mode.
     *
     * @return the initial load mode object
     */
    public InitialLoadMode getInitialLoadMode() {
        return initialLoadMode;
    }

    /**
     * Sets the initial load mode.
     * <ul>
     * <li>LAZY: Default load mode where load is async</li>
     * <li>EAGER: load mode where load is blocked till all partitions are loaded</li>
     * </ul>
     *
     * @param initialLoadMode the initial load mode object
     */
    public MapStoreConfig setInitialLoadMode(InitialLoadMode initialLoadMode) {
        this.initialLoadMode = initialLoadMode;
        return this;
    }

    /**
     * Returns {@code true} if write-coalescing is enabled.
     *
     * @return {@code true} if coalescing enabled, {@code false} otherwise
     * @see #setWriteCoalescing(boolean)
     */
    public boolean isWriteCoalescing() {
        return writeCoalescing;
    }

    /**
     * Setting {@link #writeCoalescing} is meaningful if you are using write-behind {@link com.hazelcast.core.MapStore}.
     * <p>
     * When {@link #writeCoalescing} is {@code true}, only the latest store operation on a key in the {@link #writeDelaySeconds}
     * time-window will be reflected to {@link com.hazelcast.core.MapStore}.
     * <p>
     * Default value is {@value #DEFAULT_WRITE_COALESCING}.
     *
     * @param writeCoalescing {@code true} to enable write-coalescing, {@code false} otherwise
     * @see GroupProperty#MAP_WRITE_BEHIND_QUEUE_CAPACITY
     */
    public MapStoreConfig setWriteCoalescing(boolean writeCoalescing) {
        this.writeCoalescing = writeCoalescing;
        return this;
    }

    @Override
    public String toString() {
        return "MapStoreConfig{"
                + "enabled=" + enabled
                + ", className='" + className + '\''
                + ", factoryClassName='" + factoryClassName + '\''
                + ", writeDelaySeconds=" + writeDelaySeconds
                + ", writeBatchSize=" + writeBatchSize
                + ", implementation=" + implementation
                + ", factoryImplementation=" + factoryImplementation
                + ", properties=" + properties
                + ", initialLoadMode=" + initialLoadMode
                + ", writeCoalescing=" + writeCoalescing
                + '}';
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MapStoreConfig)) {
            return false;
        }

        MapStoreConfig that = (MapStoreConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (writeCoalescing != that.writeCoalescing) {
            return false;
        }
        if (writeDelaySeconds != that.writeDelaySeconds) {
            return false;
        }
        if (writeBatchSize != that.writeBatchSize) {
            return false;
        }
        if (className != null ? !className.equals(that.className) : that.className != null) {
            return false;
        }
        if (factoryClassName != null ? !factoryClassName.equals(that.factoryClassName) : that.factoryClassName != null) {
            return false;
        }
        if (implementation != null ? !implementation.equals(that.implementation) : that.implementation != null) {
            return false;
        }
        if (factoryImplementation != null ? !factoryImplementation.equals(that.factoryImplementation)
                : that.factoryImplementation != null) {
            return false;
        }
        if (!properties.equals(that.properties)) {
            return false;
        }
        return initialLoadMode == that.initialLoadMode;
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity"})
    public final int hashCode() {
        final int prime = 31;
        int result = (enabled ? 1 : 0);
        result = prime * result + (writeCoalescing ? 1 : 0);
        result = prime * result + (className != null ? className.hashCode() : 0);
        result = prime * result + (factoryClassName != null ? factoryClassName.hashCode() : 0);
        result = prime * result + writeDelaySeconds;
        result = prime * result + writeBatchSize;
        result = prime * result + (implementation != null ? implementation.hashCode() : 0);
        result = prime * result + (factoryImplementation != null ? factoryImplementation.hashCode() : 0);
        result = prime * result + properties.hashCode();
        result = prime * result + (initialLoadMode != null ? initialLoadMode.hashCode() : 0);
        return result;
    }


    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.MAP_STORE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeBoolean(writeCoalescing);
        out.writeUTF(className);
        out.writeUTF(factoryClassName);
        out.writeInt(writeDelaySeconds);
        out.writeInt(writeBatchSize);
        out.writeObject(implementation);
        out.writeObject(factoryImplementation);
        out.writeObject(properties);
        out.writeUTF(initialLoadMode.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        enabled = in.readBoolean();
        writeCoalescing = in.readBoolean();
        className = in.readUTF();
        factoryClassName = in.readUTF();
        writeDelaySeconds = in.readInt();
        writeBatchSize = in.readInt();
        implementation = in.readObject();
        factoryImplementation = in.readObject();
        properties = in.readObject();
        initialLoadMode = InitialLoadMode.valueOf(in.readUTF());
    }
}
