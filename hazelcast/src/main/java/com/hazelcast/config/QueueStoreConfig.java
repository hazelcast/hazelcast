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

import com.hazelcast.collection.QueueStore;
import com.hazelcast.collection.QueueStoreFactory;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Configuration for the {@link QueueStore}.
 */
public class QueueStoreConfig implements IdentifiedDataSerializable {

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

    public QueueStore getStoreImplementation() {
        return storeImplementation;
    }

    public QueueStoreConfig setStoreImplementation(@Nonnull QueueStore storeImplementation) {
        this.storeImplementation = checkNotNull(storeImplementation, "Queue store cannot be null!");
        this.className = null;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public QueueStoreConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getClassName() {
        return className;
    }

    public QueueStoreConfig setClassName(@Nonnull String className) {
        this.className = checkHasText(className, "Queue store class name must contain text");
        this.storeImplementation = null;
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public QueueStoreConfig setProperties(Properties properties) {
        this.properties = checkNotNull(properties, "Queue store config properties cannot be null");
        return this;
    }

    public String getProperty(String name) {
        return properties.getProperty(name);
    }

    public QueueStoreConfig setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    public String getFactoryClassName() {
        return factoryClassName;
    }

    public QueueStoreConfig setFactoryClassName(@Nonnull String factoryClassName) {
        this.factoryClassName = checkHasText(factoryClassName, "Queue factory store class name must contain text");
        this.factoryImplementation = null;
        return this;
    }

    public QueueStoreFactory getFactoryImplementation() {
        return factoryImplementation;
    }

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
        out.writeUTF(className);
        out.writeUTF(factoryClassName);
        out.writeObject(properties);
        out.writeObject(storeImplementation);
        out.writeObject(factoryImplementation);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        enabled = in.readBoolean();
        className = in.readUTF();
        factoryClassName = in.readUTF();
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
