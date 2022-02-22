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

import com.hazelcast.collection.QueueStore;
import com.hazelcast.collection.QueueStoreFactory;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Configuration for the {@link QueueStore}.
 */
public class QueueStoreConfig implements IdentifiedDataSerializable {
    /**
     * The default number of queue item values to keep in memory. This value is
     * ignored if there is no queue store enabled or if the queue is a priority
     * queue.
     */
    public static final int DEFAULT_MEMORY_LIMIT = 1000;

    /**
     * The default size of batches in which the queue will be loaded from the
     * queue store. This value is ignored if the queue is a priority queue. In
     * that case, the queue is loaded fully during initialisation.
     */
    public static final int DEFAULT_BULK_LOAD = 250;

    /**
     * Binary: By default, Hazelcast stores the queue items in serialized form in memory.
     * Before it inserts the queue items into datastore, it deserializes them. But if you
     * will not reach the queue store from an external application, you might prefer that the
     * items be inserted in binary form. You can get rid of the de-serialization step; this
     * would be a performance optimization. The binary feature is disabled by default.
     */
    public static final String STORE_BINARY = "binary";

    /**
     * Memory Limit: This is the number of items after which Hazelcast will store items only to
     * datastore. For example, if the memory limit is 1000, then the 1001st item will be put
     * only to datastore. This feature is useful when you want to avoid out-of-memory conditions.
     * The default number for memory-limit is 1000. If you want to always use memory, you can set
     * it to Integer.MAX_VALUE.
     */
    public static final String STORE_MEMORY_LIMIT = "memory-limit";

    /**
     * Bulk Load: When the queue is initialized, items are loaded from QueueStore in bulks. Bulk
     * load is the size of these bulks. By default, bulk-load is 250.
     */
    public static final String STORE_BULK_LOAD = "bulk-load";

    private boolean enabled = true;
    private String className;
    private String factoryClassName;
    private Properties properties = new Properties();
    private QueueStore storeImplementation;
    private QueueStoreFactory factoryImplementation;

    public QueueStoreConfig() {
    }

    public QueueStoreConfig(QueueStoreConfig config) {
        enabled = config.isEnabled();
        className = config.getClassName();
        storeImplementation = config.getStoreImplementation();
        factoryClassName = config.getFactoryClassName();
        factoryImplementation = config.getFactoryImplementation();
        properties.putAll(config.getProperties());
    }

    /**
     * Returns the implementation of the queue store which will be used to store
     * queue items.
     */
    public @Nullable
    QueueStore getStoreImplementation() {
        return storeImplementation;
    }

    /**
     * Sets the implementation of the queue store which will be used to store
     * queue items.
     *
     * @param storeImplementation the implementation to store queue items
     * @return this configuration
     * @throws NullPointerException if the provided implementation is {@code null}
     */
    public QueueStoreConfig setStoreImplementation(@Nonnull QueueStore storeImplementation) {
        this.storeImplementation = checkNotNull(storeImplementation, "Queue store cannot be null!");
        this.className = null;
        return this;
    }

    /**
     * Returns {@code true} if the queue store is enabled, {@code false}
     * otherwise.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables or disables the queue store.
     *
     * @param enabled {@code true} to enable the queue store, {@code false} to disable it
     * @return this configuration
     */
    public QueueStoreConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns the class name of the queue store implementation to be used when
     * instantiating the queue store.
     */
    public @Nullable
    String getClassName() {
        return className;
    }

    /**
     * Sets the class name of the queue store implementation to be used when
     * instantiating the queue store. The class should implement the
     * {@link QueueStore} interface.
     *
     * @param className the queue store class name
     * @return this configuration
     */
    public QueueStoreConfig setClassName(@Nonnull String className) {
        this.className = checkHasText(className, "Queue store class name must contain text");
        this.storeImplementation = null;
        return this;
    }

    /**
     * Returns the properties to be used when instantiating the queue store. Some
     * properties are used by Hazelcast to determine how to interact with the
     * queue store (see {@link QueueStoreConfig#STORE_MEMORY_LIMIT},
     * {@link QueueStoreConfig#STORE_BINARY} and
     * {@link QueueStoreConfig#STORE_BULK_LOAD}).
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the properties to be used when instantiating the queue store. Some
     * properties are used by Hazelcast to determine how to interact with the
     * queue store (see {@link QueueStoreConfig#STORE_MEMORY_LIMIT},
     * {@link QueueStoreConfig#STORE_BINARY} and
     * {@link QueueStoreConfig#STORE_BULK_LOAD}).
     *
     * @param properties the properties to be used when instantiating the queue store
     * @return this configuration
     */
    public QueueStoreConfig setProperties(Properties properties) {
        this.properties = checkNotNull(properties, "Queue store config properties cannot be null");
        return this;
    }

    /**
     * Returns the property with the given {@code name} which is used when
     * instantiating and interacting with the queue store.
     *
     * @param name the property name
     * @return the property value
     */
    public String getProperty(String name) {
        return properties.getProperty(name);
    }

    /**
     * Sets a property to be used when instantiating the queue store. Some
     * properties are used by Hazelcast to determine how to interact with the
     * queue store (see {@link QueueStoreConfig#STORE_MEMORY_LIMIT},
     * {@link QueueStoreConfig#STORE_BINARY} and
     * {@link QueueStoreConfig#STORE_BULK_LOAD}).
     *
     * @param name  the property name
     * @param value the property value
     * @return this configuration
     */
    public QueueStoreConfig setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    /**
     * Returns the factory class name which will be used to instantiate the queue
     * store. The class should implement the {@link QueueStoreFactory} interface.
     */
    public @Nullable
    String getFactoryClassName() {
        return factoryClassName;
    }

    /**
     * Sets the factory class name which will be used to instantiate the queue
     * store. The class should implement the {@link QueueStoreFactory} interface.
     *
     * @param factoryClassName the queue store factory class name
     * @return this configuration
     */
    public QueueStoreConfig setFactoryClassName(@Nonnull String factoryClassName) {
        this.factoryClassName = checkHasText(factoryClassName, "Queue factory store class name must contain text");
        this.factoryImplementation = null;
        return this;
    }

    /**
     * Returns the queue store factory implementation which will be used to
     * instantiate the queue store. The class should implement the
     * {@link QueueStoreFactory} interface.
     */
    public @Nullable
    QueueStoreFactory getFactoryImplementation() {
        return factoryImplementation;
    }

    /**
     * Sets the queue store factory implementation which will be used to
     * instantiate the queue store. The class should implement the
     * {@link QueueStoreFactory} interface.
     *
     * @param factoryImplementation the queue store factory implementation
     * @return this configuration
     */
    public QueueStoreConfig setFactoryImplementation(@Nonnull QueueStoreFactory factoryImplementation) {
        this.factoryImplementation = checkNotNull(factoryImplementation, "Queue store factory cannot be null!");
        this.factoryClassName = null;
        return this;
    }

    public String toString() {
        return "QueueStoreConfig{"
                + "enabled=" + enabled
                + ", className='" + className + '\''
                + ", storeImplementation=" + storeImplementation
                + ", factoryClassName='" + factoryClassName + '\''
                + ", factoryImplementation=" + factoryImplementation
                + ", properties=" + properties
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.QUEUE_STORE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeString(className);
        out.writeString(factoryClassName);
        out.writeObject(properties);
        out.writeObject(storeImplementation);
        out.writeObject(factoryImplementation);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        enabled = in.readBoolean();
        className = in.readString();
        factoryClassName = in.readString();
        properties = in.readObject();
        storeImplementation = in.readObject();
        factoryImplementation = in.readObject();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueueStoreConfig)) {
            return false;
        }

        QueueStoreConfig that = (QueueStoreConfig) o;
        return isEnabled() == that.isEnabled()
                && Objects.equals(getProperties(), that.getProperties())
                && Objects.equals(className, that.className)
                && Objects.equals(storeImplementation, that.storeImplementation)
                && Objects.equals(factoryClassName, that.factoryClassName)
                && Objects.equals(factoryImplementation, that.factoryImplementation);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(isEnabled(), className, storeImplementation, getProperties(), factoryClassName,
                factoryImplementation);
    }
}
